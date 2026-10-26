package protocol

import (
	"fmt"
	"strings"
)

// sixbitAlphabet is the 64-symbol character set used by FreeText. Index 0 is
// space so that zeroed padding decodes to blanks.
const sixbitAlphabet = " ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.,-/:?!'()+=#@&*;\"_<>%$[]^|"

var sixbitIndex = func() (m [256]int8) {
	for i := range m {
		m[i] = -1
	}
	for i := 0; i < len(sixbitAlphabet); i++ {
		m[sixbitAlphabet[i]] = int8(i)
	}
	return
}()

var turkishFold = strings.NewReplacer(
	"ç", "C", "Ç", "C", "ğ", "G", "Ğ", "G", "ı", "I", "İ", "I",
	"ö", "O", "Ö", "O", "ş", "S", "Ş", "S", "ü", "U", "Ü", "U",
)

// NormalizeText upper-cases s and folds Turkish letters into the 6-bit
// alphabet. It does not validate; EncodeSixbit does.
func NormalizeText(s string) string {
	return strings.ToUpper(turkishFold.Replace(s))
}

func sixbitCodes(s string) ([]uint8, error) {
	out := make([]uint8, len(s))
	for i := 0; i < len(s); i++ {
		c := sixbitIndex[s[i]]
		if c < 0 {
			return nil, fmt.Errorf("%w: character %q not in 6-bit alphabet", ErrInvalidField, s[i])
		}
		out[i] = uint8(c)
	}
	return out, nil
}
