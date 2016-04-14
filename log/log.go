package log

import (
	syslog "log"
)

// Level of logging
type Level int

// Levels
const (
	LevelDebug Level = 1 + iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// crude global log level control:
// log everything at this level and above
var logLevel = LevelDebug
var logLevelStack []Level

// LevelPush push current level down the stack and set  
func LevelPush( level Level) {
    logLevelStack = append(logLevelStack, level)
    logLevel = level
}

// LevelPop pop current level from the stack   
func LevelPop() {
    len := len(logLevelStack)
    logLevel, logLevelStack = logLevelStack[len-1], logLevelStack[:len-1]  
}

// Debug log if debug is greater than current log level
func Debug(msg ...interface{}) {
	if LevelDebug >= logLevel {
		syslog.Println("D:", msg)
	}
}

// Info log if info is greater than current log level
func Info(msg ...interface{}) {
	if LevelInfo >= logLevel {
		syslog.Println("I:", msg)
	}
}

// Warn log if warn is greater than current log level
func Warn(msg ...interface{}) {
	if LevelWarn >= logLevel {
		syslog.Println("W:", msg)
	}
}

// Error log if error is greater than current log level
func Error(msg ...interface{}) {
	if LevelError >= logLevel {
		syslog.Println("E:", msg)
	}
}

// Fatal log unconditionally and panic
func Fatal(msg ...interface{}) {
	syslog.Fatalln("F:", msg)
}
