package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/executor"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/storage"
	"github.com/kelseyhightower/envconfig"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// O tipo decInt é necessário pois a biblioteca converte usando ParseInt passando
// zero na base. Ou seja, meses como 08 passam a ser inválidos pois são tratados
// como números octais.
type decInt int

func (i *decInt) Decode(value string) error {
	v, err := strconv.Atoi(value)
	*i = decInt(v)
	return err
}

type config struct {
	Month     decInt `envconfig:"MONTH"`
	Year      decInt `envconfig:"YEAR"`
	AID       string `envconfig:"AID"`
	SuccCodes []int  `envconfig:"SUCC_CODES"`

	MongoURI    string `envconfig:"MONGODB_URI"`
	DBName      string `envconfig:"MONGODB_DBNAME"`
	MongoMICol  string `envconfig:"MONGODB_MICOL"`
	MongoErrCol string `envconfig:"MONGODB_ERRCOL"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL"`
	MongoRevCol string `envconfig:"MONGODB_REVCOL"`

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
	var pExec executor.PipelineExecution
	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}
	if err := prototext.Unmarshal(erIN, &pExec); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error reading execution result: %v", err)))
	}
	agmi := storage.AgencyMonthlyInfo{
		AgencyID:          strings.ToLower(c.AID),
		Month:             int(c.Month),
		Year:              int(c.Year),
		CrawlingTimestamp: timestamppb.Now(),
	}
	for _, r := range pExec.Results {
		switch r.Status {
		case executor.StageExecution_SETUP_ERROR:
			agmi.ProcInfo = stepExec2ProcInfo(r.Setup)
			agmi.ExectionTime = float64(r.Setup.FinishTime.AsTime().Sub(r.Setup.StartTime.AsTime()).Milliseconds())
		case executor.StageExecution_BUILD_ERROR:
			agmi.ProcInfo = stepExec2ProcInfo(r.Build)
			agmi.ExectionTime = float64(r.Setup.FinishTime.AsTime().Sub(r.Setup.StartTime.AsTime()).Milliseconds())
		case executor.StageExecution_RUN_ERROR:
			agmi.ProcInfo = stepExec2ProcInfo(r.Run)
			agmi.ExectionTime = float64(r.Setup.FinishTime.AsTime().Sub(r.Setup.StartTime.AsTime()).Milliseconds())
		case executor.StageExecution_TEARDOWN_ERROR:
			agmi.ProcInfo = stepExec2ProcInfo(r.Teardown)
			agmi.ExectionTime = float64(r.Setup.FinishTime.AsTime().Sub(r.Setup.StartTime.AsTime()).Milliseconds())
		}
	}
	miCol := c.MongoErrCol
	if contains(c.SuccCodes, int(agmi.ProcInfo.Status)) {
		miCol = c.MongoMICol
	}
	client, err := newClient(c, miCol)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("newClient() error: %s", err)))
	}
	if err = client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi: %v", err)))
	}
}

// newClient Creates client to connect with DB and Cloud5
func newClient(conf config, miCol string) (*storage.Client, error) {
	db, err := storage.NewDBClient(conf.MongoURI, conf.DBName, miCol, conf.MongoAgCol, conf.MongoPkgCol, conf.MongoRevCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(miCol)
	bc := storage.NewCloudClient(conf.SwiftUsername, conf.SwiftAPIKey, conf.SwiftAuthURL, conf.SwiftDomain, conf.SwiftContainer)
	client, err := storage.NewClient(db, bc)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}

func stepExec2ProcInfo(se *executor.StepExecution) *coleta.ProcInfo {
	return &coleta.ProcInfo{
		Cmd:    se.Cmd,
		CmdDir: se.CmdDir,
		Stdin:  se.Stdin,
		Stdout: se.Stdout,
		Stderr: se.Stderr,
		Status: se.StatusCode,
		Env:    se.Env,
	}
}

func contains(succCodes []int, s int) bool {
	for _, c := range succCodes {
		if s == c {
			return true
		}
	}
	return false
}
