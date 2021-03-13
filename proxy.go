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
	proxyAddress := "127.0.0.1:13306"
	backendAddress := "127.0.0.1:3306"
	username := "root"
	password := "root"

	listen, err := net.Listen("tcp", proxyAddress)
	if err != nil {
		log.Error().Err(err).Str("proxyAddress", proxyAddress).Msg("failed to listen")
		panic(err)
	}

	handlerMap := map[string]*ProxyHandler{}
	startAccept := true

	go func() {
		for {
			if !startAccept {
				log.Warn().Msg("stop accept")
				return
			}

			log.Info().Msg("wait client connect")

			accept, err := listen.Accept()
			if err != nil {
				log.Error().Err(err).Msg("failed to accept")
				continue
			}

			go func() {
				req := uuid.NewString()
				logger := log.With().Str("req", req).Interface("remote", accept.RemoteAddr()).Logger()
				logger.Info().Msg("connected client")

				defer func() {
					err := accept.Close()
					if err != nil {
						if netErr, ok := err.(*net.OpError); ok && netErr.Op == "close" {
							logger.Info().Msg("closed client")
						} else {
							logger.Warn().Err(err).Msg("failed to close accept")
						}
					}
				}()

				handler := NewProxyHandler(logger)

				conn, err := server.NewConn(accept, username, password, handler)
				if err != nil {
					logger.Error().Err(err).Msg("failed to login mysql")
					return
				}

				err = handler.Open(backendAddress, username, password)
				if err != nil {
					logger.Error().Err(err).Msg("failed to create handler")
					return
				}

				defer func() {
					err := handler.Close()
					if err != nil {
						logger.Error().Err(err).Msg("failed to close handler")
					}
				}()

				handlerMap[req] = handler
				defer delete(handlerMap, req)

				for {
					err := conn.HandleCommand()
					if err != nil {
						logger.Error().Err(err).Msg("failed to handle mysql command")
						return
					}
				}
			}()
		}
	}()

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	receivedOsSignal := <-osSignal
	log.Info().Interface("receivedOsSignal", receivedOsSignal).Msg("received os signal")

	startAccept = false
	err = listen.Close()
	if err != nil {
		log.Warn().Err(err).Msg("failed to close listen")
	}

	wg := sync.WaitGroup{}
	for req, handler := range handlerMap {
		wg.Add(1)

		go func(req string, handler *ProxyHandler) {
			defer wg.Done()

			handler.Close()
			log.Info().Str("req", req).Msg("closed connection")
		}(req, handler)
	}
	wg.Wait()
}
