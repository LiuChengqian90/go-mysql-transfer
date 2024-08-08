package nets

import (
	"fmt"
	"testing"
)

func TestIsUsableAddr(t *testing.T) {
	fmt.Println(IsUsableTcpAddr(":8080"))
	fmt.Println(IsUsableTcpAddr(":8070"))
	fmt.Println(IsUsableTcpAddr(":8060"))
	fmt.Println(IsUsableTcpAddr(":8050"))
}
