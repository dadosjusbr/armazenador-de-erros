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
	"github.com/dadosjusbr/storage/models"
	"github.com/dadosjusbr/storage/repositories/database/mongo"
	"github.com/dadosjusbr/storage/repositories/database/postgres"
	"github.com/dadosjusbr/storage/repositories/fileStorage"
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

	PostgresUser     string `envconfig:"POSTGRES_USER" required:"true"`
	PostgresPassword string `envconfig:"POSTGRES_PASSWORD" required:"true"`
	PostgresDBName   string `envconfig:"POSTGRES_DBNAME" required:"true"`
	PostgresHost     string `envconfig:"POSTGRES_HOST" required:"true"`
	PostgresPort     string `envconfig:"POSTGRES_PORT" required:"true"`

	AWSRegion    string `envconfig:"AWS_REGION" required:"true"`
	S3Bucket     string `envconfig:"S3_BUCKET" required:"true"`
	AWSAccessKey string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecretKey string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`

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
	agmi := models.AgencyMonthlyInfo{
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
	// Criando o client do MongoDB
	mongoDb, err := mongo.NewMongoDB(c.MongoURI, c.DBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol, c.MongoRevCol)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating MongoDB client: %v", err.Error())))
	}
	mongoDb.Collection(miCol)

	// Criando o client do Postgres
	postgresDB, err := postgres.NewPostgresDB(c.PostgresUser, c.PostgresPassword, c.PostgresDBName, c.PostgresHost, c.PostgresPort)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating PostgresDB client: %v", err.Error())))
	}

	// Criando o client do S3
	s3Client, err := fileStorage.NewS3Client(c.AWSRegion, c.S3Bucket)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error creating S3 client: %v", err.Error())))
	}

	// Criando client do storage a partir do banco postgres e do client do s3
	pgS3Client, err := storage.NewClient(postgresDB, s3Client)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up postgres storage client: %s", err)))
	}
	defer pgS3Client.Db.Disconnect()

	// Criando o client do storage a partir do banco mongodb e do client do s3
	mgoS3Client, err := storage.NewClient(mongoDb, s3Client)
	if err != nil {
		status.ExitFromError(status.NewError(3, fmt.Errorf("error setting up mongo storage client: %s", err)))
	}
	defer mgoS3Client.Db.Disconnect()

	if err = mgoS3Client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi in mongo: %v", err)))
	}
	if err = pgS3Client.Store(agmi); err != nil {
		status.ExitFromError(status.NewError(2, fmt.Errorf("error trying to store agmi in postgres: %v", err)))
	}
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
