package model

import (
	"github.com/BurntSushi/toml"
)

type TOMLConfig struct {
	Database struct {
		Path        string `toml:"path"`
		Raw         string `toml:"raw"`
		Token       string `toml:"token"`
		Cache       string `toml:"cache"`
		PriceDBPath string `toml:"price_db_path"`
	} `toml:"database"`
	Search struct {
		OutDegreeLimit int `toml:"out_degree_limit"`
		Depth          int `toml:"depth"`
	} `toml:"search"`
	Flow struct {
		ActivateThreshold float64 `toml:"activate_threshold"`
		AgeLimit          int     `toml:"age_limit"`
		LabelLimit        int     `toml:"label_limit"`
	} `toml:"flow"`
	Log struct {
		Level string `toml:"level"`
	} `toml:"log"`
	Cex struct {
		AddressFile map[string]string `toml:"address_file"` // cex_name -> file_path
	} `toml:"cex"`
	Path struct {
		Depth          int     `toml:"depth"`
		MaxPathGlobal  int     `toml:"max_path_global"`
		MaxPathPerpair int     `toml:"max_path_perpair"`
		MinValue       float64 `toml:"min_value"`
		MaxTime        int     `toml:"max_time"`
	} `toml:"path"`
	Aws struct {
		Bucket           string `toml:"bucket"`
		Prefix           string `toml:"prefix"`
		FetchConcurrency int    `toml:"fetch_concurrency"`
		BatchSize        int    `toml:"batch_size"`
		StartBlockId     uint16 `toml:"start_block_id"`
		EndBlockId       uint16 `toml:"end_block_id"`
		StartDate        string `toml:"start_date"`
		EndDate          string `toml:"end_date"`
		MinTransferValue int    `toml:"min_transfer_value"`
	} `toml:"aws"`
}

var GlobalTomlConfig *TOMLConfig

func InitGlobalTomlConfig() {
	if GlobalTomlConfig == nil {
		GlobalTomlConfig = &TOMLConfig{}
		if _, err := toml.DecodeFile("config.toml", GlobalTomlConfig); err != nil {
			panic(err)
		}
	}
}

func GetConfigDBPath() string {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Database.Path
}

func GetConfigPriceDBPath() string {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Database.PriceDBPath
}

func GetConfigRawPath() string {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Database.Raw
}

func GetConfigTokenPath() string {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Database.Token
}

func GetConfigOutDegreeLimit() int {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Search.OutDegreeLimit
}

func GetConfigSearchDepth() int {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Search.Depth
}

func GetConfigFlowActivateThreshold() float64 {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Flow.ActivateThreshold
}

func GetConfigLogLevel() string {
	var tomlConfig TOMLConfig
	if _, err := toml.DecodeFile("config.toml", &tomlConfig); err != nil {
		panic(err)
	}
	return tomlConfig.Log.Level
}
