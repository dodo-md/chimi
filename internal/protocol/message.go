package protocol

import "time"

type Message struct {
	Sender    string    `json:"sender"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

func NewMessage(sender, content string) Message {
	return Message{
		Sender:    sender,
		Content:   content,
		Timestamp: time.Now(),
	}
}
