package api

type TransmissionConnection struct {
	Address string `json:"address"`
}

type TransmissionConnectResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message"`
	Data    TransmissionConnection `json:"data"`
}
