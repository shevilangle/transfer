package transfer

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"log"
)

type Transfer struct {
	Pool           *redis.Pool
	FromString     string
	PrefixToString string
	Receivers      func([]byte) (error, []string, []interface{})
	SaveToDB       func(o interface{})
}

func NewTransfer(p *redis.Pool, f string, pt string, r func([]byte) (error, []string, []interface{}), s func(interface{})) *Transfer {
	return &Transfer{Pool: p, FromString: f, PrefixToString: pt, Receivers: r, SaveToDB: s}
}

func (t *Transfer) rab() {
	conn := t.Pool.Get()
	defer conn.Close()
	psc := redis.PubSubConn{conn}
	psc.Subscribe(t.FromString)
	for {
		switch n := psc.Receive().(type) {
		case redis.Message:
			log.Println("Message,channel :", n.Channel, ", data :", n.Data)
			errs, subscribers, datas := t.Receivers(n.Data)
			if errs == nil {
				log.Println("s: ", subscribers)
				if len(subscribers) > 0 {
					t.b(subscribers, datas)
				}
			} else {
				panic(errs)
			}
		case redis.PMessage:
			log.Println("PMessage, pattern: ", n.Pattern, ", channel :", n.Channel, ", data :", n.Data)
		case redis.Subscription:
			log.Println("Subscription, kind: ", n.Kind, ", channel:", n.Channel, ", count:", n.Count)
			if n.Count == 0 {
				return
			}
		case error:
			log.Println("error: ", n)
			return
		}
	}
}

func (t *Transfer) b(r []string, es []interface{}) {
	conn := t.Pool.Get()
	defer conn.Close()

	for _, data := range es {
		log.Println("data: ", data)
		t.SaveToDB(data)
	}

	for i, receiver := range r {
		log.Println("receiver: ", t.PrefixToString+receiver)
		pubData, err := json.Marshal(es[i])
		if err != nil {
			log.Println("error: ", err)
			return
		}
		log.Println("pubData: ", pubData)
		_, err = conn.Do("PUBLISH", t.PrefixToString+receiver, pubData)
		log.Println("err: ", err)
	}
}

func (t *Transfer) Push() {
	t.rab()
}
