package player

import "fmt"

func errOutOfRange(pos, length int) error {
	return fmt.Errorf("seek position %d out of range [0,%d]", pos, length)
}
