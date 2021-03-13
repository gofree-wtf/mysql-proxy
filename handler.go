package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

type ProxyHandler struct {
	logger zerolog.Logger
	conn   *client.Conn
}

func NewProxyHandler(logger zerolog.Logger) *ProxyHandler {
	return &ProxyHandler{
		logger: logger,
	}
}

func (h *ProxyHandler) Open(address string, username string, password string) error {
	conn, err := client.Connect(address, username, password, "")
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to connect mysql")
		return err
	}

	h.conn = conn
	h.logger.Info().Msg("success to connect mysql")
	return nil
}

func (h *ProxyHandler) Close() error {
	err := h.conn.Close()
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to close mysql")
		return err
	}
	return nil
}

//handle COM_INIT_DB command, you can check whether the dbName is valid, or other.
func (h *ProxyHandler) UseDB(dbName string) error {
	err := h.conn.UseDB(dbName)
	if err != nil {
		h.logger.Error().Err(err).Str("database", dbName).Msg("failed to use database")
		return err
	}
	return nil
}

//handle COM_QUERY command, like SELECT, INSERT, UPDATE, etc...
//If Result has a Resultset (SELECT, SHOW, etc...), we will send this as the response, otherwise, we will send Result
func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	result, err := h.conn.Execute(query)
	if err != nil {
		h.logger.Error().Err(err).Str("query", query).Msg("failed to query")
		return nil, err
	}
	return result, nil
}

//handle COM_FILED_LIST command
func (h *ProxyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	fields, err := h.conn.FieldList(table, fieldWildcard)
	if err != nil {
		h.logger.Error().Err(err).Str("table", table).Str("fieldWildcard", fieldWildcard).
			Msg("failed to get fields")
		return nil, err
	}
	return fields, nil
}

//handle COM_STMT_PREPARE, params is the param number for this statement, columns is the column number
//context will be used later for statement execute
func (h *ProxyHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	return 0, 0, nil, fmt.Errorf("not supported now")
}

//handle COM_STMT_EXECUTE, context is the previous one set in prepare
//query is the statement prepare query, and args is the params for this statement
func (h *ProxyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, fmt.Errorf("not supported now")
}

//handle COM_STMT_CLOSE, context is the previous one set in prepare
//this handler has no response
func (h *ProxyHandler) HandleStmtClose(context interface{}) error {
	return nil
}

//handle any other command that is not currently handled by the library,
//default implementation for this method will return an ER_UNKNOWN_ERROR
func (h *ProxyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}
