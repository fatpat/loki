package stages

import (
	"reflect"
    "strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

// Config Errors
const (
	ErrEmptyLtsvStageConfig = "empty ltsv stage configuration"
	ErrEmptyLtsvStageSource = "empty source"
)

// LtsvConfig contains a ltsvStage configuration
type LtsvConfig struct {
	Source     *string `mapstructure:"source"`
}

// validateLtsvConfig validates the config and return a ltsv
func validateLtsvConfig(c *LtsvConfig) (error) {
	if c == nil {
		return errors.New(ErrEmptyLtsvStageConfig)
	}

	if c.Source != nil && *c.Source == "" {
		return errors.New(ErrEmptyLtsvStageSource)
	}

	return nil
}

// ltsvStage sets extracted data using regular expressions
type ltsvStage struct {
	cfg        *LtsvConfig
	logger     log.Logger
}

// newLtsvStage creates a newLtsvStage
func newLtsvStage(logger log.Logger, config interface{}) (Stage, error) {
	cfg, err := parseLtsvConfig(config)
	if err != nil {
		return nil, err
	}
	err = validateLtsvConfig(cfg)
	if err != nil {
		return nil, err
	}
	return toStage(&ltsvStage{
		cfg:        cfg,
		logger:     log.With(logger, "component", "stage", "type", "ltsv"),
	}), nil
}

// parseLtsvConfig processes an incoming configuration into a LtsvConfig
func parseLtsvConfig(config interface{}) (*LtsvConfig, error) {
	cfg := &LtsvConfig{}
	err := mapstructure.Decode(config, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Process implements Stage
func (r *ltsvStage) Process(labels model.LabelSet, extracted map[string]interface{}, t *time.Time, entry *string) {
	// If a source key is provided, the ltsv stage should process it
	// from the extracted map, otherwise should fallback to the entry
	input := entry

	if r.cfg.Source != nil {
		if _, ok := extracted[*r.cfg.Source]; !ok {
			if Debug {
				level.Debug(r.logger).Log("msg", "source does not exist in the set of extracted values", "source", *r.cfg.Source)
			}
			return
		}

		value, err := getString(extracted[*r.cfg.Source])
		if err != nil {
			if Debug {
				level.Debug(r.logger).Log("msg", "failed to convert source value to string", "source", *r.cfg.Source, "err", err, "type", reflect.TypeOf(extracted[*r.cfg.Source]))
			}
			return
		}

		input = &value
	}

	if input == nil {
		if Debug {
			level.Debug(r.logger).Log("msg", "cannot parse a nil entry")
		}
		return
	}

    kvs := strings.Split(*input, "\t")
    for i := range kvs {
        res := strings.SplitN(kvs[i], ":", 2)
        if len(res) == 2 {
            extracted[res[0]] = res[1]
        }
    }
}

// Name implements Stage
func (r *ltsvStage) Name() string {
	return StageTypeLtsv
}
