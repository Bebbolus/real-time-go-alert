package main

import (
        "log"
        "github.com/go-telegram-bot-api/telegram-bot-api"
        "github.com/segmentio/kafka-go"
        "context"
        "strings"
        "time"
        "encoding/json"
        "io/ioutil"
        "os"
        "os/signal"
        "strconv"
        "github.com/elastic/go-elasticsearch"
)

type configurations struct {
        Telegram struct {
                Chatid int64  `json:"chatid"`
                Botapi string `json:"botapi"`
        } `json:"telegram"`
        Kafka struct {
                ServerPort string `json:"server:port"`
                GroupId    string `json:"group:id"`
                Protocol   string `json:"protocol"`
                Topic      string `json:"topic"`
                Partition  int `json:"partition"`
        } `json:"kafka"`
        MessageLevel int `json:"message:level"`
}

//ReadFromJSON function load a json file into a struct or return error
func ReadFromJSON(t interface{}, filename string) error {

        jsonFile, err := ioutil.ReadFile(filename)
        if err != nil {
                return err
        }
        err = json.Unmarshal([]byte(jsonFile), t)
        if err != nil {
                log.Fatalf("error: %v", err)
                return err
        }

        return nil
}

func readFromKafka(c chan string) {
    log.Print(">>> starting kafka listner")

    // make a new reader that consumes from topic
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   []string{Conf.Kafka.ServerPort},
        GroupID:   Conf.Kafka.GroupId,
        Topic:     Conf.Kafka.Topic,
       // MinBytes:  10e3, // 10KB
       // MaxBytes:  10e6, // 10MB
    })

    for {
        m, err := r.ReadMessage(context.Background())
        if err != nil {
            break
        }
        log.Print(">>> received message")
        c <- string(m.Value)
    }

    r.Close()
}

type Kfkmsg struct {
    Level int `json:"level"`
    Msg string `json:"message"`
}

func sendToKafka(msk Kfkmsg){
    mapB, _ := json.Marshal(msk)
    log.Print(">>> connecting to kafka...")
    conn, _ := kafka.DialLeader(context.Background(), Conf.Kafka.Protocol, Conf.Kafka.ServerPort, Conf.Kafka.Topic, Conf.Kafka.Partition)
    conn.SetWriteDeadline(time.Now().Add(10*time.Second))
    log.Print(">>> sending message to kafka")
    conn.WriteMessages(kafka.Message{Value: []byte(string(mapB))},)
    conn.Close()
    log.Print(">>> message sent")
}

func listenTl(bot *tgbotapi.BotAPI){
    log.Print(">>> initializing telegram listner")
    u := tgbotapi.NewUpdate(0)
    u.Timeout = 60

    updates, _ := bot.GetUpdatesChan(u)
    for update := range updates {
        if update.ChannelPost != nil {
            log.Print(">>> received channel message")

            var tosend Kfkmsg
            tosend.Level = 2
            tosend.Msg= update.ChannelPost.Text
            sendToKafka(tosend)
        }

        if update.Message != nil {
            sresult := esSearch(update.Message.Text)
            log.Print("received results:")
            //log.Print(sresult)
            for _, resmsg := range sresult {
                log.Print(resmsg)
                out, err := json.Marshal(resmsg)
                if err != nil {
                    panic (err)
                }
                sendToTelegram(bot, string(out))
            }
        }

    }
}

func esSearch (textToSearch string) []esResult{
    var r  map[string]interface{}

    // Initialize a client with the default settings.
    es, err := elasticsearch.NewDefaultClient()
    if err != nil {
        log.Fatalf("Error creating the client: %s", err)
    }

    // Use the helper methods of the client.
    res, err := es.Search(
        es.Search.WithContext(context.Background()),
        es.Search.WithIndex("*"),
        es.Search.WithBody(strings.NewReader(`{"query" : { "match" : { "message" : "`+ textToSearch +`"  } }}`)),
        //es.Search.WithTrackTotalHits(true),
        es.Search.WithPretty(),
    )
    if err != nil {
        log.Fatalf("ERROR: %s", err)
    }
    defer res.Body.Close()

    if res.IsError() {
        var e map[string]interface{}
        if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
            log.Fatalf("error parsing the response body: %s", err)
        } else {
            // Print the response status and error information.
            log.Fatalf("[%s] %s: %s",
                res.Status(),
                e["error"].(map[string]interface{})["type"],
                e["error"].(map[string]interface{})["reason"],
            )
        }
    }

    if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
        log.Fatalf("Error parsing the response body: %s", err)
    }

    // Print the response status, number of results, and request duration.
    log.Printf(
        "[%s] %d hits; took: %dms",
        res.Status(),
        int(r["hits"].(map[string]interface{})["total"].(float64)),//.(map[string]interface{})["value"].(float64)),
        //int(r["took"].(float64)),
    )

    var final []esResult

    // return the document source for each hit.
    for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
        final = append(final, esResult{Id: hit.(map[string]interface{})["_id"].(string), Index: hit.(map[string]interface{})["_index"].(string), Message: hit.(map[string]interface{})["_source"].(map[string]interface{})["message"].(string)})
        //log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
    }

    //log.Print(textToSearch)
    return final
}

type esResult struct{
    Id string
    Index string
    Message string
}


//Helper
func must(err error) {
        if err != nil {
                log.Print(err)
                os.Exit(1)
        }
}

var Conf configurations
func sendToTelegram(bot *tgbotapi.BotAPI, msg string){
    sendmsg := tgbotapi.NewMessage(Conf.Telegram.Chatid, msg)
    bot.Send(sendmsg)
}

func main() {
    must(ReadFromJSON(&Conf, "conf.json"))
    bot, err := tgbotapi.NewBotAPI(Conf.Telegram.Botapi)
    if err != nil {
        log.Panic(err)
    }

    bot.Debug = false
    log.Printf("Authorized on account %s", bot.Self.UserName)

    u := tgbotapi.NewUpdate(0)
    u.Timeout = 60

    //START READER
    sendToTelegram(bot, "a listner is started")

    ckill := make(chan os.Signal, 1)
    signal.Notify(ckill, os.Interrupt)
    go func(){
        for _ = range ckill {
            // sig is a ^C, handle it
            sendmsg := tgbotapi.NewMessage(Conf.Telegram.Chatid, "listner closed")
            bot.Send(sendmsg)
            os.Exit(0)
        }
    }()

    c := make(chan string)
    go readFromKafka(c)
    go listenTl(bot)
    for i := range c {
        var sourceMsg Kfkmsg
        if json.Unmarshal([]byte(i), &sourceMsg) == nil  {
            if sourceMsg.Level >= Conf.MessageLevel || Conf.MessageLevel == 0 {
                t := time.Now()
                avviso :=[]string{"!ALERT @",t.Format("2006-01-02 15:04:05"), "\r\n >Level: ",strconv.Itoa(sourceMsg.Level), "\r\n >Message:", sourceMsg.Msg}
                messaggio := strings.Join(avviso, "")
                newmsg := tgbotapi.NewMessage(Conf.Telegram.Chatid, messaggio)
                bot.Send(newmsg)
            }
        } else {
                log.Printf("!ERROR: not compatible JSON string received!:  %s ", string(i))
        }
    }
}





