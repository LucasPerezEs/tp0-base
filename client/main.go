package main

import (
	"fmt"
	"os"
	"strings"
	"os/signal"
	"syscall"

	"github.com/op/go-logging"
	"github.com/spf13/viper"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("batch", "maxAmount")
	v.BindEnv("log", "level")
	v.BindEnv("first_name")
	v.BindEnv("last_name")
	v.BindEnv("document")
	v.BindEnv("birthdate")
	v.BindEnv("number")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./app/config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %d | server_address: %s | log_level: %s | batch_max_amount: %d",
		v.GetInt("id"),
		v.GetString("server.address"),
		v.GetString("log.level"),
		v.GetInt("batch.maxAmount"),
	)
}

// PrintBetInfo Print all the bet information of the client. For debugging purposes only
func PrintBetInfo(clientBet domain.Bet) {
	log.Infof("action: bet_info | result: success | first_name: %s | last_name: %s | document: %s | birthdate: %s | number: %v",
		clientBet.FirstName,
		clientBet.LastName,
		clientBet.Document,
		clientBet.Birthdate,
		clientBet.Number,
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	// Handle SIGTERM signal to gracefully shutdown the client
	signalChannel := make(chan os.Signal, 1)
	
	signal.Notify(signalChannel, syscall.SIGTERM)

	clientConfig := common.ClientConfig{
		ServerAddress: v.GetString("server.address"),
		ID:            uint8(v.GetInt("id")),
		BatchMaxAmount: v.GetInt("batch.maxAmount"),
	}

	clientBet := domain.Bet{
		FirstName:      v.GetString("first_name"),
		LastName:       v.GetString("last_name"),
		Document:      v.GetString("document"),
		Birthdate: v.GetString("birthdate"),
		Number: v.GetInt("number"),
	}

	// Print bet info with debugging purposes
	PrintBetInfo(clientBet)
	
	client := common.NewClient(clientConfig, clientBet)
	client.StartClient(signalChannel)
}
