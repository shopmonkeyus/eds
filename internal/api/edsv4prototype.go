package api

type EdsV4PrototypeConnection struct {
	Address string `json:"address"`
}

type EdsV4PrototypeConnectResponse struct {
	Success bool                     `json:"success"`
	Message string                   `json:"message"`
	Data    EdsV4PrototypeConnection `json:"data"`
}
