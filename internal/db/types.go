package db

type Row struct {
	PageId      string `json:"pageID"`
	ComponentId string `json:"element"`
	Timestamp   string `json:"timestamp"`
	Event       string `json:"event"`
	UpdatedAt   string `json:"updated_at"`
	CreatedAt   string `json:"created_at"`
}
