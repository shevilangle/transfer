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
	EventCountStr  string
	Receivers      func([]byte) (error, []string, []interface{}, string)
	SaveToDB       func(o interface{})
}

func NewTransfer(p *redis.Pool, f string, pt string, evcs string, r func([]byte) (error, []string, []interface{}, string), s func(interface{})) *Transfer {
	if r == nil {
		return nil
	}
	return &Transfer{Pool: p, FromString: f, PrefixToString: pt, EventCountStr: evcs, Receivers: r, SaveToDB: s}
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
			errs, subscribers, datas, et := t.Receivers(n.Data)
			if errs == nil {
				log.Println("s: ", subscribers)
				if len(subscribers) > 0 {
					t.b(subscribers, datas, et)
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

func (t *Transfer) b(r []string, es []interface{}, et string) {
	conn := t.Pool.Get()
	defer conn.Close()

	for _, data := range es {
		log.Println("data: ", data)
		if t.SaveToDB != nil {
			t.SaveToDB(data)
		}
	}

	for i, receiver := range r {
		log.Println("receiver: ", t.PrefixToString+receiver)
		conn.Do("HINCRBY", t.EventCountStr+receiver, et, 1)

		pubData, err := json.Marshal(es[i])
		if err != nil {
			log.Println("error: ", err)
			return
		}
		//log.Println("pubData: ", pubData)
		_, err = conn.Do("PUBLISH", t.PrefixToString+receiver, pubData)
		log.Println("err: ", err)
	}
}

func (t *Transfer) Push() {
	t.rab()
}
