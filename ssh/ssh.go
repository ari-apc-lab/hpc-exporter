package ssh

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	log "github.com/sirupsen/logrus"
)

type SSHCommand struct {
	Path string
	Env  []string
}

type SSHConfig struct {
	Config *ssh.ClientConfig
	Host   string
	Port   int
}

type SSHClient struct {
	*ssh.Client
}

type SSHSession struct {
	*ssh.Session
	InBuffer  *bytes.Buffer
	OutBuffer *bytes.Buffer
	ErrBuffer *bytes.Buffer
}

func NewSSHConfigByPassword(user, password, host string, port int) *SSHConfig {

	log.Info("Trying to create a SSHConfig of type NewSSHConfigByPassword...")

	return &SSHConfig{
		Config: &ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				ssh.Password(password),
			},
			Timeout:         10 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
		Host: host,
		Port: port,
	}
}

func NewSSHConfigByPublicKeys(user, host string, port int, key []byte) *SSHConfig {

	// A public key may be used to authenticate against the remote
	// server by using an unencrypted PEM-encoded private key file.
	//
	// If you have an encrypted private key, the crypto/x509 package
	// can be used to decrypt it.

	log.Info("Trying to create a SSHConfig of type NewSSHConfigByPublicKeys...")

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		log.Fatalf("unable to parse private key: %v", err)
	} else {
		log.Info("Signer created for the parsed private key")
	}

	return &SSHConfig{
		Config: &ssh.ClientConfig{
			User:            user,
			Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
			Timeout:         10 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
		Host: host,
		Port: port,
	}
}

func NewSSHConfigByAgent(user, host string, port int) *SSHConfig {
	return &SSHConfig{
		Config: &ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				SSHAgent(),
			},
			Timeout:         10 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
		Host: host,
		Port: port,
	}
}

func (config *SSHConfig) NewClient() (*SSHClient, error) {
	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port), config.Config)
	if err != nil {
		return nil, err
	}
	return &SSHClient{client}, nil
}

func (client *SSHClient) OpenSession(inBuffer, outBuffer, errBuffer *bytes.Buffer) (*SSHSession, error) {
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,     // disable echoing
		ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}

	if err := session.RequestPty("xterm", 80, 40, modes); err != nil {
		session.Close()
		return nil, err
	}

	// Setup the buffers
	ses := &SSHSession{Session: session, InBuffer: inBuffer, OutBuffer: outBuffer, ErrBuffer: errBuffer}
	if err := ses.setupSessionBuffers(); err != nil {
		session.Close()
		return nil, err
	}
	return ses, nil
}

func (s *SSHSession) CloseSession() {

	log.Debugf("Closing session")

	err := s.Close()
	if err == nil {
		log.Debugf("Session closed successfully")
	} else if err == io.EOF {
		log.Debugf("Session had already been closed: %s", err.Error())
	} else {
		log.Errorf("Session could not be closed properly: %s", err.Error())
	}
}

func (session *SSHSession) setupSessionBuffers() error {
	if session.InBuffer != nil {
		stdin, err := session.StdinPipe()
		if err != nil {
			return err
		}
		go io.Copy(stdin, session.InBuffer)
	}

	if session.OutBuffer != nil {
		stdout, err := session.StdoutPipe()
		if err != nil {
			return err
		}
		go io.Copy(session.OutBuffer, stdout)
	}

	if session.ErrBuffer != nil {
		stderr, err := session.StderrPipe()
		if err != nil {
			return err
		}
		go io.Copy(session.ErrBuffer, stderr)
	}

	return nil
}

func ExecuteSSHCommand(cmd string, client *SSHClient) *SSHSession {
	command := &SSHCommand{
		Path: cmd,
		// Env:    []string{"LC_DIR=/usr"},
	}

	var outb, errb bytes.Buffer
	session, err := client.OpenSession(nil, &outb, &errb)
	if err == nil {
		if err2 := session.RunCommand(command); err2 == nil {
			return session
		} else {
			err_chunk, err3 := session.ErrBuffer.ReadString('\n')
			errSsh := err_chunk
			for ; err3 == nil; errSsh += err_chunk {
				err_chunk, err3 = session.ErrBuffer.ReadString('\n')
			}
			err_chunk, err3 = session.OutBuffer.ReadString('\n')
			errSsh += err_chunk
			for ; err3 == nil; errSsh += err_chunk {
				err_chunk, err3 = session.OutBuffer.ReadString('\n')
			}
			log.Errorf("Error: %s when executing SSH Command: %s", err2.Error(), cmd)
			log.Debugf("Error was: " + errSsh)
			return nil
		}
	} else {
		log.Errorf("Error: %s when executing SSH Command: %s", err, cmd)
		log.Debugf("Error opening session")
		return nil
	}
}

func (session *SSHSession) RunCommand(cmd *SSHCommand) error {
	if err := session.setupCommand(cmd); err != nil {
		return err
	}

	err := session.Run(cmd.Path)
	return err
}

func (session *SSHSession) setupCommand(cmd *SSHCommand) error {
	// TODO(emepetres) clear env before setting a new one?
	for _, env := range cmd.Env {
		variable := strings.Split(env, "=")
		if len(variable) != 2 {
			continue
		}

		if err := session.Setenv(variable[0], variable[1]); err != nil {
			return err
		}
	}

	return nil
}

func PublicKeyFile(file string) ssh.AuthMethod {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return nil
	}

	key, err := ssh.ParsePrivateKey(buffer)
	if err != nil {
		return nil
	}
	return ssh.PublicKeys(key)
}

func SSHAgent() ssh.AuthMethod {
	if sshAgent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		return ssh.PublicKeysCallback(agent.NewClient(sshAgent).Signers)
	}
	return nil
}
