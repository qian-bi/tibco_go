package emsmsg

/*
#cgo CFLAGS: -I../include
#cgo LDFLAGS: -L../lib -ltibems
#include "../include/tibems/tibems.h"

extern void callback(tibemsMsgConsumer, tibemsMsg, void *);
*/
import "C"
import (
	"fmt"
	"unsafe"
)

var errorContext C.tibemsErrorContext
var factory C.tibemsConnectionFactory
var connection C.tibemsConnection
var session C.tibemsSession
var status C.tibems_status
var _debug bool
var connected bool

func init() {
	errorContext = nil
	factory = nil
	connection = nil
	session = nil
	status = C.TIBEMS_OK
	_debug = false
	connected = false
}

// CreateSession .
func CreateSession(serverURL string, userName string, password string, debug bool) {
	status = C.tibemsErrorContext_Create(&errorContext)
	checkStatus()
	factory = C.tibemsConnectionFactory_Create()
	if factory == nil {
		panic(fmt.Sprint("Create factory Error"))
	}
	s := C.CString(serverURL)
	defer C.free(unsafe.Pointer(s))
	u := C.CString(userName)
	defer C.free(unsafe.Pointer(u))
	p := C.CString(password)
	defer C.free(unsafe.Pointer(p))
	status = C.tibemsConnectionFactory_SetServerURL(factory, s)
	checkStatus()
	status = C.tibemsConnectionFactory_CreateConnection(factory, &connection, u, p)
	checkStatus()
	status = C.tibemsConnection_CreateSession(connection, &session, C.TIBEMS_FALSE, C.TIBEMS_AUTO_ACKNOWLEDGE)
	checkStatus()
	_debug = debug
	connected = true
}

// CloseSession .
func CloseSession() {
	if session != nil {
		status = C.tibemsSession_Close(session)
		checkStatus()
		session = nil
	}
	if connection != nil {
		status = C.tibemsConnection_Close(connection)
		checkStatus()
		connection = nil
	}
	if factory != nil {
		status = C.tibemsConnectionFactory_Destroy(factory)
		checkStatus()
		factory = nil
	}
	if errorContext != nil {
		status = C.tibemsErrorContext_Close(errorContext)
		errorContext = nil
	}
	connected = false
}

// Producer .
func Producer(dest string, useTopic bool, data string, msgType string, name string) {
	var destination C.tibemsDestination
	var msgProducer C.tibemsMsgProducer
	var msg C.tibemsMsg
	checkConnection()
	createDest(&destination, dest, useTopic)
	status = C.tibemsSession_CreateProducer(session, &msgProducer, destination)
	checkStatus()
	createMsg(&msg, data, msgType, name)
	if _debug {
		C.tibemsMsg_Print(msg)
	}
	status = C.tibemsMsgProducer_Send(msgProducer, msg)
	checkStatus()
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsgProducer_Close(msgProducer)
	checkStatus()
	destroyDest(destination)
}

// Consumer .
func Consumer(dest string, useTopic bool, timeout int) string {
	var destination C.tibemsDestination
	var msgConsumer C.tibemsMsgConsumer
	var msg C.tibemsMsg
	checkConnection()
	createDest(&destination, dest, useTopic)
	status = C.tibemsSession_CreateConsumer(session, &msgConsumer, destination, nil, C.TIBEMS_FALSE)
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
	status = C.tibemsMsgConsumer_ReceiveTimeout(msgConsumer, &msg, C.longlong(timeout*1000))
	checkStatus()
	res := getMsg(msg)
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsgConsumer_Close(msgConsumer)
	checkStatus()
	destroyDest(destination)
	return res
}

// Requester .
func Requester(dest string, useTopic bool, data string, msgType string, name string) string {
	var destination C.tibemsDestination
	var msgRequestor C.tibemsMsgRequestor
	var msg C.tibemsMsg
	var reply C.tibemsMsg
	checkConnection()
	createDest(&destination, dest, useTopic)
	status = C.tibemsMsgRequestor_Create(session, &msgRequestor, destination)
	checkStatus()
	createMsg(&msg, data, msgType, name)
	if _debug {
		C.tibemsMsg_Print(msg)
	}
	status = C.tibemsMsgRequestor_Request(msgRequestor, msg, &reply)
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
	res := getMsg(reply)
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsg_Destroy(reply)
	checkStatus()
	status = C.tibemsMsgRequestor_Close(msgRequestor)
	checkStatus()
	destroyDest(destination)
	return res
}

// Listener .
func Listener(dest string, useTopic bool, closure func(string)) {
	var destination C.tibemsDestination
	var msgConsumer C.tibemsMsgConsumer
	checkConnection()
	createDest(&destination, dest, useTopic)
	status = C.tibemsSession_CreateConsumer(session, &msgConsumer, destination, nil, C.TIBEMS_FALSE)
	checkStatus()
	status = C.tibemsMsgConsumer_SetMsgListener(msgConsumer, C.tibemsMsgCallback(C.callback), unsafe.Pointer(&closure))
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
}

// UnSubscribe .
func UnSubscribe(name string) {
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	status = C.tibemsSession_Unsubscribe(session, n)
	checkStatus()
}

// Subscriber .
func Subscriber(topic string, name string, selector string, timeout int) string {
	var destination C.tibemsDestination
	var msgConsumer C.tibemsMsgConsumer
	var msg C.tibemsMsg
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	s := C.CString(selector)
	defer C.free(unsafe.Pointer(s))
	checkConnection()
	createDest(&destination, topic, true)
	status = C.tibemsSession_CreateDurableSubscriber(session, (*C.tibemsTopicSubscriber)(&msgConsumer), C.tibemsTopic(destination), n, s, C.TIBEMS_FALSE)
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
	status = C.tibemsMsgConsumer_ReceiveTimeout(msgConsumer, &msg, C.longlong(timeout*1000))
	checkStatus()
	res := getMsg(msg)
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsgConsumer_Close(msgConsumer)
	checkStatus()
	destroyDest(destination)
	return res
}

// SharedConsumer .
func SharedConsumer(topic string, name string, selector string, timeout int) string {
	var destination C.tibemsDestination
	var msgConsumer C.tibemsMsgConsumer
	var msg C.tibemsMsg
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	s := C.CString(selector)
	defer C.free(unsafe.Pointer(s))
	checkConnection()
	createDest(&destination, topic, true)
	status = C.tibemsSession_CreateSharedConsumer(session, &msgConsumer, C.tibemsTopic(destination), n, s)
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
	status = C.tibemsMsgConsumer_ReceiveTimeout(msgConsumer, &msg, C.longlong(timeout*1000))
	checkStatus()
	res := getMsg(msg)
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsgConsumer_Close(msgConsumer)
	checkStatus()
	destroyDest(destination)
	return res
}

// SharedSubscriber .
func SharedSubscriber(topic string, name string, selector string, timeout int) string {
	var destination C.tibemsDestination
	var msgConsumer C.tibemsMsgConsumer
	var msg C.tibemsMsg
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	s := C.CString(selector)
	defer C.free(unsafe.Pointer(s))
	checkConnection()
	createDest(&destination, topic, true)
	status = C.tibemsSession_CreateSharedDurableConsumer(session, &msgConsumer, C.tibemsTopic(destination), n, s)
	checkStatus()
	status = C.tibemsConnection_Start(connection)
	checkStatus()
	status = C.tibemsMsgConsumer_ReceiveTimeout(msgConsumer, &msg, C.longlong(timeout*1000))
	checkStatus()
	res := getMsg(msg)
	status = C.tibemsMsg_Destroy(msg)
	checkStatus()
	status = C.tibemsMsgConsumer_Close(msgConsumer)
	checkStatus()
	destroyDest(destination)
	return res
}

func checkStatus() {
	if status != C.TIBEMS_OK {
		statusText := C.tibemsStatus_GetText(status)
		var errMsg, errTraceback *C.char
		C.tibemsErrorContext_GetLastErrorString(errorContext, &errMsg)
		C.tibemsErrorContext_GetLastErrorStackTrace(errorContext, &errTraceback)
		panic(fmt.Sprintf("Status: %s\nError message: %s\nTrack back: %s\n", C.GoString(statusText), C.GoString(errMsg), C.GoString(errTraceback)))
	}
}

func checkConnection() {
	if !connected {
		panic("Connection is closed")
	}
}

func getMsgTypeName(msgType C.tibemsMsgType) string {
	var msgTypeName string
	switch msgType {
	case C.TIBEMS_MESSAGE:
		msgTypeName = "MESSAGE"
	case C.TIBEMS_BYTES_MESSAGE:
		msgTypeName = "BYTES"
	case C.TIBEMS_OBJECT_MESSAGE:
		msgTypeName = "OBJECT"
	case C.TIBEMS_STREAM_MESSAGE:
		msgTypeName = "STREAM"
	case C.TIBEMS_MAP_MESSAGE:
		msgTypeName = "MAP"
	case C.TIBEMS_TEXT_MESSAGE:
		msgTypeName = "TEXT"
	default:
		msgTypeName = "UNKNOWN"
	}
	return msgTypeName
}

func createDest(destination *C.tibemsDestination, dest string, useTopic bool) {
	d := C.CString(dest)
	defer C.free(unsafe.Pointer(d))
	if useTopic {
		status = C.tibemsDestination_Create(destination, C.TIBEMS_TOPIC, d)
	} else {
		status = C.tibemsDestination_Create(destination, C.TIBEMS_QUEUE, d)
	}
	checkStatus()
}

func destroyDest(destination C.tibemsDestination) {
	status = C.tibemsDestination_Destroy(destination)
	checkStatus()
}

func getMsg(msg C.tibemsMsg) string {
	var msgType C.tibemsMsgType
	var enumeration C.tibemsMsgEnum
	var name *C.char
	var txt *C.char
	if _debug {
		C.tibemsMsg_Print(msg)
	}
	status = C.tibemsMsg_GetBodyType(msg, &msgType)
	checkStatus()
	switch msgType {
	case C.TIBEMS_TEXT_MESSAGE:
		status = C.tibemsTextMsg_GetText(msg, &txt)
		checkStatus()
	case C.TIBEMS_MAP_MESSAGE:
		status = C.tibemsMapMsg_GetMapNames(msg, &enumeration)
		checkStatus()
		status = C.tibemsMsgEnum_GetNextName(enumeration, &name)
		checkStatus()
		status = C.tibemsMapMsg_GetString(msg, name, &txt)
		checkStatus()
		status = C.tibemsMsgEnum_Destroy(enumeration)
		checkStatus()
	default:
		msgTypeName := getMsgTypeName(msgType)
		panic(fmt.Sprintf("Unknown message type %s", msgTypeName))
	}
	return C.GoString(txt)
}

func createMsg(msg *C.tibemsMsg, data string, msgType string, name string) {
	d := C.CString(data)
	defer C.free(unsafe.Pointer(d))
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))
	switch msgType {
	case "TEXT":
		status = C.tibemsTextMsg_Create((*C.tibemsTextMsg)(msg))
		checkStatus()
		status = C.tibemsTextMsg_SetText(*msg, d)
		checkStatus()
	case "MAP":
		status = C.tibemsMapMsg_Create((*C.tibemsMapMsg)(msg))
		checkStatus()
		status = C.tibemsMapMsg_SetString(*msg, n, d)
		checkStatus()
	}
}

//export callback
func callback(msgConsumer C.tibemsMsgConsumer, msg C.tibemsMsg, closure unsafe.Pointer) {
	res := getMsg(msg)
	(*(*func(string))(closure))(res)
}
