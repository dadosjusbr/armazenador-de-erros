package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/storage"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI   string `envconfig:"MONGODB_URI"`
	DBName     string `envconfig:"MONGODB_DBNAME"`
	MongoMICol string `envconfig:"MONGODB_MICOL"`
	MongoAgCol string `envconfig:"MONGODB_AGCOL"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL"`
	// Swift Conf
	SwiftUsername  string `envconfig:"SWIFT_USERNAME"`
	SwiftAPIKey    string `envconfig:"SWIFT_APIKEY"`
	SwiftAuthURL   string `envconfig:"SWIFT_AUTHURL"`
	SwiftDomain    string `envconfig:"SWIFT_DOMAIN"`
	SwiftContainer string `envconfig:"SWIFT_CONTAINER"`
}

func main() {
	var c config
	if err := envconfig.Process("", &c); err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error loading config values from .env: %v", err.Error())))
	}
	client, err := newClient(c)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("newClient() error: %s", err)))
	}
	var er pipeline.ResultadoExecucao
	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}
	if err = json.Unmarshal(erIN, &er); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}

	agmi := storage.AgencyMonthlyInfo{
		AgencyID: er.Rc.Coleta.Orgao,
		Month:    int(er.Rc.Coleta.Mes),
		Year:     int(er.Rc.Coleta.Ano),
	}
	var packBackup *storage.Backup
	var backup []storage.Backup
	if er.Pr.Pacote != "" {
		packBackup, err = client.Cloud.UploadFile(er.Pr.Pacote, er.Rc.Coleta.Orgao)
		if err != nil {
			status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup package files: %v, error: %v", er.Pr.Pacote, err)))
		}
		agmi.Package = packBackup
	}
	if er.Rc.Coleta.Arquivos != nil {
		backup, err = client.Cloud.Backup(er.Rc.Coleta.Arquivos, er.Rc.Coleta.Orgao)
		if err != nil {
			status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to get Backup files: %v, error: %v", er.Rc.Coleta.Arquivos, err)))
		}
		agmi.Backups = backup
	}

	if er.Rc.Procinfo != nil && er.Rc.Procinfo.Status != 0 {
		agmi.ProcInfo = er.Rc.Procinfo
	}

	if err = client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi: %v", err)))
	}
}

// newClient Creates client to connect with DB and Cloud5
func newClient(conf config) (*storage.Client, error) {
	db, err := storage.NewDBClient(conf.MongoURI, conf.DBName, conf.MongoMICol, conf.MongoAgCol, conf.MongoPkgCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(conf.MongoMICol)
	bc := storage.NewCloudClient(conf.SwiftUsername, conf.SwiftAPIKey, conf.SwiftAuthURL, conf.SwiftDomain, conf.SwiftContainer)
	client, err := storage.NewClient(db, bc)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}
