package pricedb

import "github.com/tinylib/msgp/msgp"

func EncodePrice(price float64) []byte {
	b := make([]byte, 0)
	o := msgp.Require(b, msgp.GuessSize(price))
	o = msgp.AppendFloat64(o, price)
	return o
}

func DecodePrice(b []byte) (float64, error) {
	price, _, err := msgp.ReadFloat64Bytes(b)
	if err != nil {
		return 0, err
	} else {
		return price, nil
	}
}

func EncodeDecimal(decimal uint8) []byte {
	b := make([]byte, 0)
	o := msgp.Require(b, msgp.GuessSize(decimal))
	o = msgp.AppendByte(o, decimal)
	return o
}

func DecodeDecimal(b []byte) (uint8, error) {
	decimal, _, err := msgp.ReadByteBytes(b)
	if err != nil {
		return 0, err
	} else {
		return decimal, nil
	}
}
