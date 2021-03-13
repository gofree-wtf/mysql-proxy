package main

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/siddontang/go-mysql/server"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	address := "127.0.0.1:13306"
	username := "root"
	password := ""

	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Error().Err(err).Str("address", address).Msg("failed to listen")
		panic(err)
	}

	connMap := map[string]*server.Conn{}

	go func() {
		for {
			accept, err := listen.Accept()
			if err != nil {
				log.Error().Err(err).Msg("failed to accept")
				continue
			}

			conn, err := server.NewConn(accept, username, password, server.EmptyHandler{})
			if err != nil {
				log.Error().Err(err).Msg("failed to connect")
				continue
			}

			connUuid := uuid.NewString()
			connMap[connUuid] = conn

			go func(uuid string, conn *server.Conn) {
				defer func() {
					delete(connMap, uuid)
					conn.Close()
				}()

				for {
					err := conn.HandleCommand()
					if err != nil {
						log.Error().Err(err).Str("uuid", uuid).Msg("failed to handle command")
						return
					}
				}
			}(connUuid, conn)
		}
	}()

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	receivedOsSignal := <-osSignal
	log.Info().Interface("receivedOsSignal", receivedOsSignal).Msg("received os signal")

	err = listen.Close()
	log.Warn().Err(err).Msg("failed to close listen")

	wg := sync.WaitGroup{}
	for connUuid, conn := range connMap {
		wg.Add(1)

		go func(uuid string, conn *server.Conn) {
			defer wg.Done()

			conn.Close()
			log.Info().Str("uuid", uuid).Msg("closed connection")
		}(connUuid, conn)
	}
	wg.Wait()
}
