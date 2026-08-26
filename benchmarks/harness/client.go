package harness

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type respClient struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dialRESP(addr string, timeout time.Duration) (*respClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &respClient{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}, nil
}

func (c *respClient) close() { _ = c.conn.Close() }

func writeCommand(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func (c *respClient) command(args ...string) (string, error) {
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := writeCommand(c.w, args...); err != nil {
		return "", err
	}
	if err := c.w.Flush(); err != nil {
		return "", err
	}
	return readResponse(c.r)
}

func readResponse(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("invalid RESP response")
	}
	switch line[0] {
	case '+', '-', ':':
		return line, nil
	case '$':
		n, err := strconv.Atoi(strings.TrimSuffix(line[1:], "\r\n"))
		if err != nil {
			return "", fmt.Errorf("bulk length: %w", err)
		}
		if n < 0 {
			return line, nil
		}
		body := make([]byte, n+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return "", err
		}
		if string(body[n:]) != "\r\n" {
			return "", errors.New("invalid bulk terminator")
		}
		return line + string(body), nil
	default:
		return "", fmt.Errorf("unsupported RESP response prefix %q", line[0])
	}
}

func responseOK(response string) bool { return response != "" && response[0] != '-' }
