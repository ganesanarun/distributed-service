package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func NewHttpServer(addr string) *http.Server {
	httpServ := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", httpServ.handleProduce).Methods(http.MethodPost)
	router.HandleFunc("/", httpServ.handleConsume).Methods(http.MethodGet)
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

func (s *httpServer) handleProduce(writer http.ResponseWriter, request *http.Request) {

	var req ProduceRequest
	if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	u, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	response := ProduceResponse{Offset: u}
	if err = json.NewEncoder(writer).Encode(response); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(writer http.ResponseWriter, request *http.Request) {

	var req ConsumeRequest
	if err := json.NewDecoder(request.Body).Decode(&req); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offset)

	if err != nil && err == ErrOffsetNotFound {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	response := ConsumeResponse{Record: record}
	if err = json.NewEncoder(writer).Encode(response); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}
