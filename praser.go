package xmqclientgo

type msgArg struct {
	topic string
	sub   string
	size  int
}

const (
	OP_STRAT = iota
	OP_M
	OP_MS
	OP_MSG
	MSG_ARG
	MSG_PAYLOAD
	MSG_END
	OP_P
	OP_PI
	OP_PIN
	OP_PING
)

func (c *Client) prase(buf []byte) error {
	for i, b := range buf {
		switch c.ps.state {
		case OP_STRAT:
			switch b {
			case 'M', 'm':
				c.ps.state = OP_M
			}
		case OP_M:
			switch b {
			case 'S', 's':
				c.ps.state = OP_MS
			default:
				goto parseErr
			}
		case OP_MS:
			switch b {
			case 'G', 'g':
				c.ps.state = OP_MSG
			default:
				goto parseErr
			}
		case OP_MSG:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.ps.state = MSG_ARG
				c.ps.as = i
			}
		case MSG_ARG:
			switch b {
			case '\r':
				c.ps.drop = 1
			case '\t':
				var arg []byte
				if c.ps.argBuf != nil {
					arg = c.ps.argBuf
				} else {
					arg = buf[c.ps.as : i-c.ps.drop]
				}
				if err := c.processMsgArg(arg); err != nil {
					return err
				}
				c.ps.drop, c.ps.state, c.ps.as = 0, MSG_PAYLOAD, i+1
			default:
				if c.ps.argBuf != nil {
					c.ps.argBuf = append(c.ps.argBuf, b)
				}
			}
		case MSG_PAYLOAD:
			if c.ps.msgBuf != nil {
				if len(c.ps.msgBuf) > c.ps.ma.size {
					c.processMsg(c.ps.msgBuf)
					c.ps.argBuf, c.ps.msgBuf, c.ps.state = nil, nil, MSG_END
				} else {
					c.ps.msgBuf = append(c.ps.msgBuf, b)
				}
			} else if i-c.ps.as >= c.ps.ma.size {
				c.processMsg(c.ps.msgBuf)
				c.ps.argBuf, c.ps.msgBuf, c.ps.state = nil, nil, MSG_END
			}
		case MSG_END:
			switch b {
			case '\n':
				c.ps.drop, c.ps.as, c.ps.state = 0, i+1, OP_STRAT
			default:
				continue
			}
		}
	}
parseErr:
	return nil
}
