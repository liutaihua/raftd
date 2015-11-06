package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"
	"bufio"
	"github.com/gorilla/websocket"
	"strconv"
	"io"
	"crypto/md5"
)

const (
	NewUserMessageType	= "message"
	UserOnlineType		= "user_online"
	UserOfflineType		= "user_offline"
)

// 节点自己持有即可，user只会在某个节点出现，不需要再做集群的一致性处理
type User struct {
	name	   		string
	msg_channel		string
	ws				*websocket.Conn
	s				*Server
	recc			chan interface {}
	stopped			chan bool
	mux				sync.Mutex
}

// Message封装的是来往各node之间的非raft消息的， 应用层消息
// github.com/gorilla/schema 从POST请求中的form里直接读取到struct数据
type Message struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	Username string      `json:"username"`
	Message  interface{} `json:"message"`
}

type raftMsg struct {
	Key		string			`json:"key"`
	Value	interface {}	`json:"value"`
}

func NewUser(name, mc string, s *Server, ws *websocket.Conn) *User {
	return &User{
		name:			name,
		msg_channel:	mc,
		ws:				ws,
		s:				s,
		stopped:		make(chan bool, 1),
		recc:			make(chan interface {}, 8),
	}
}

func MakeRandomID() string {
	nano := time.Now().UnixNano()
	rand.Seed(nano)
//	rndNum := rand.Int63()

	md5 := md5.New()
	io.WriteString(md5, strconv.FormatInt(nano, 10))
	md5_nano := md5.Sum(nil)
	return string(md5_nano)
}

func (u *User) UpdateOnline() {
	msg := Message{
		ID:			MakeRandomID(),
		Type: 		UserOnlineType,
		Username:	u.name,
		Message:	"",
	}
	buf, _ := json.Marshal(msg)
	u.s.raftServer.Do(raft.NewWriteCommand(u.name, string(buf)))
}

func (u *User) UpdateOffline() {
	fmt.Println("UpdateOffline-------------", u.name)
	msg := Message{
		ID:			MakeRandomID(),
		Type: 		UserOfflineType,
		Username:	u.name,
		Message:	"",
	}
	buf, _ := json.Marshal(msg)
	u.s.raftServer.Do(raft.NewWriteCommand(u.name, string(buf)))
}

func (u *User) msgLoop() {
	fmt.Println("entery msgLoop", u.name)
	defer func() {fmt.Println("fuck-------------------------------")}()
	for {
		select {
		case m := <-u.recc:
			fmt.Println("User msgLoop got", m.(Message))
			buf, err := json.Marshal(m.(Message))
			if err != nil {
				fmt.Println("json.Marshal err", err.Error())
				continue
			}
			if u.ws == nil {
				fmt.Println("not found user websocket conn", u.name)
				continue
			}
			u.ws.WriteMessage(websocket.TextMessage, buf)
		case <-u.stopped:
			fmt.Println("```````````````````stopped user`````````````````````````")
			return
		}
	}
}

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	db         *raft.DB
	users	   map[string]*User
	mutex      sync.RWMutex
}

// Creates a new server.
func New(path string, host string, port int) *Server {
	s := &Server{
		host:   host,
		port:   port,
		path:   path,
		db:     raft.NewDB(),
		users:	make(map[string]*User),
		router: mux.NewRouter(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}
//	go s.lookUser()
	return s
}

func (s *Server) lookUser() {
	for {
		for _, u := range s.users {
			fmt.Println("exist user:", u.name, "nil ws:", u.ws==nil, len(u.stopped))
		}
		time.Sleep(time.Second*3)
	}
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func (s *Server) OnMsgRec() <-chan interface {} {
	return s.raftServer.GetRec()
}

func (s *Server) AddUser(user *User) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.users[user.name] = user
}

func (s *Server) RemoveUser(user *User) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	fmt.Println("oooooooooooooo", s.users, user.name)
	delete(s.users, user.name)
}

func (s *Server) GetUser(name string) *User {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	user, ok := s.users[name]
	if !ok {
		return nil
	}
	return user
}

func (s *Server) doUserOnline(name string) {
	user := s.GetUser(name)
	if user == nil {
		fmt.Println("not found user when doUserOnline, will create it", name)
//		user = NewUser(name, "", s, nil)
//		s.AddUser(user)
		return
	}
	go user.msgLoop()
	fmt.Println("now user online:", user.name)
}

func (s *Server) doNewMsg(m Message) {
	user := s.GetUser(m.Username)
	if user == nil {
		fmt.Println("not found user:", m.Username, "for msg:", m)
		return
	}
	fmt.Println("doNewMsg", m.Username, len(user.recc), len(user.stopped))
	go func() {
		user.recc <- m
		fmt.Println("send m to user.recc done")
	}()
	fmt.Println("doNewMsg done")
}

func (s *Server) doUserOffline(name string) {
	user := s.GetUser(name)
	if user != nil {
		user.ws = nil
		// stop the user loop
		s.RemoveUser(user)
		user.stopped <- true
	}
}

func (s *Server) ListenRaftAppendedEntrys() {
	for {
		select {
		case m := <-s.OnMsgRec():
			fmt.Println("app server got raft appended", m)
			/* TODO process this msg, like user online, django's message to user
			cache the message in memory wait for user online
			update userlist struct
			update user message
			set flag to judge message that expired
			*/
			var msg Message
			var rmsg raftMsg
			err := json.Unmarshal([]byte(m.(string)), &rmsg)
			if err != nil {
				fmt.Println("111unmarshall err", err.Error())
			}

			err = json.Unmarshal([]byte(rmsg.Value.(string)), &msg)
			if err != nil {
				fmt.Println("222unmarshall err", err.Error())
			}

			switch msg.Type {
			case NewUserMessageType:
				fmt.Println("new user message", msg)
				s.doNewMsg(msg)
			case UserOnlineType:
				fmt.Println("user online msg", msg)
				s.doUserOnline(msg.Username)
			case UserOfflineType:
				fmt.Println("user offline msg", msg)
				s.doUserOffline(msg.Username)
			default:
				fmt.Println("default type", msg)
			}
		}
	}
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.Printf("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", "db", 10*time.Millisecond)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.db, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	go s.ListenRaftAppendedEntrys()

	if leader != "" {
		// Join to leader if specified.

		log.Println("Attempting to join leader:", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Fatal(err)
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}

	log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	// raft自用的几个接口
	s.router.HandleFunc("/db/{key}", s.readHandler).Methods("GET")
	s.router.HandleFunc("/db/{key}", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	s.router.HandleFunc("/serv/push", s.WsReadHandler).Methods("GET")
	s.router.HandleFunc("/channel/{channel}", s.NewUserMessageHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())

	return s.httpServer.ListenAndServe()
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	value := s.db.Get(vars["key"])
	w.Write([]byte(value))
}

var upgrader = websocket.Upgrader{ReadBufferSize:  4096, WriteBufferSize: 4096, CheckOrigin:     ReqCheckOrigin}
func ReqCheckOrigin(req *http.Request) bool {
	return true
}

func (s *Server) WsReadHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Println("WsReadHandler start...")
	ws, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		fmt.Println("upgrade error")
		return
	}
	// For debugging purpose, get remote IP
	remote_addr := req.Header.Get("X-Forwarded-For")
	if remote_addr == "" {
		remote_addr = req.RemoteAddr
	}

	ws_query := req.URL.Query()
	fmt.Println("ws_query:", ws_query)
	var username, channel string
	if value, ok := ws_query["username"]; ok {
		username = value[0]
	} else {
		return
	}

	if value, ok := ws_query["channel"]; ok {
		channel = value[0]
	} else {
		return
	}

	if username == "" || channel == "" {
		return
	}
	user := NewUser(username, channel, s, ws)
	s.AddUser(user)
	user.UpdateOnline()
	defer func() {
		fmt.Println("a websocket finished")
		user.UpdateOffline()
		ws.Close()
	}()

	for {
		ws.SetReadDeadline(time.Now().Add(300 * time.Second))
		_, r, err := ws.NextReader()
		if err != nil {
			fmt.Println("Read Failed:", err)
			return
		}
		reader := bufio.NewReader(r)
		data, err := reader.ReadBytes('\n')
		if err != nil {
			fmt.Println("WebSocket Server: Websocket Read Failed:", err, data)
			return
			//			break
			//				continue
		}
	}
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	// Execute the command against the Raft server.
	_, err = s.raftServer.Do(raft.NewWriteCommand(vars["key"], value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func (s *Server) NewUserMessageHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	if channel, ok := vars["channel"]; !ok {
		fmt.Println(channel)
		http.Error(w, "channel name not in url", 400)
		return
	}
	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	// Execute the command against the Raft server.
	_, err = s.raftServer.Do(raft.NewWriteCommand(vars["key"], value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

