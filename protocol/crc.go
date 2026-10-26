package protocol

// CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF, no reflection, no xorout.
// Check value for "123456789" is 0x29B1.

var crcTable = func() (t [256]uint16) {
	for i := range t {
		c := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if c&0x8000 != 0 {
				c = c<<1 ^ 0x1021
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return
}()

// CRC16 computes CRC-16/CCITT-FALSE over b.
func CRC16(b []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, x := range b {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^x]
	}
	return crc
}
