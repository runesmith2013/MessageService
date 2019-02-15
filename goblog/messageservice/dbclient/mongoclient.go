package dbclient

import (
	"github.com/rs/xid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"messageservice/goblog/messageservice/model"
)

type MongoClient struct {
	session *mgo.Session
}

func (mc *MongoClient) Connect() {

	var err error
	mc.session, err = mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}

	mc.ensureIndex()

	mc.session.SetMode(mgo.Monotonic, true)

}

func (mc *MongoClient) ensureIndex() {
	session := mc.session.Copy()
	defer session.Close()

	c := session.DB("store").C("topics")

	index := mgo.Index{
		Key:        []string{"id"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
	}
	err := c.EnsureIndex(index)
	if err != nil {
		panic(err)
	}
}

func (mc *MongoClient) GetTopics() ([]model.Topic, error) {

	session := mc.session.Copy()

	c := session.DB("store").C("topics")

	var topics []model.Topic
	error := c.Find(bson.M{}).All(&topics)

	return topics, error

}

func (mc *MongoClient) AddTopic(topic model.Topic) error {

	session := mc.session.Copy()
	defer session.Close()

	topic.Id = xid.New().String()
	c := session.DB("store").C("topics")

	err := c.Insert(topic)

	return err
}
