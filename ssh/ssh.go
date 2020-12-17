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
	knownhosts "golang.org/x/crypto/ssh/knownhosts"

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

/*
func NewSSHConfigByCertificate(user, key_file, host string, port int) *SSHConfig {
	return &SSHConfig{
		Config: &ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				PublicKeyFile(key_file),
			},
			Timeout:         10 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
		Host: host,
		Port: port,
	}
}
*/

func NewSSHConfigByPublicKeys(user, host string, port int, private_key_path string, known_hosts_path string) *SSHConfig {

        // A public key may be used to authenticate against the remote
        // server by using an unencrypted PEM-encoded private key file.
        //
        // If you have an encrypted private key, the crypto/x509 package
        // can be used to decrypt it.

	log.Info("Trying to create a SSHConfig of type NewSSHConfigByPublicKeys...")

        key, err := ioutil.ReadFile(private_key_path)
        if err != nil {
                log.Fatalf("unable to read private key: %v", err)
        } else {
		log.Info("Local private key file read")
	}

        // Create the Signer for this private key.
        signer, err := ssh.ParsePrivateKey(key)
        if err != nil {
                log.Fatalf("unable to parse private key: %v", err)
	} else {
		log.Info("Signer created for the parsed private key")
	}

        hostKeyCallback, err := knownhosts.New(known_hosts_path)
        if err != nil {
                log.Fatalf("unable to parse local known_hosts file: %v", err)
	} else {
		log.Info("Local known_hosts file parsed")
	}

        return &SSHConfig {
                Config: &ssh.ClientConfig {
                        User: user,
                        Auth: []ssh.AuthMethod{
                        // Use the PublicKeys method for remote authentication.
                                ssh.PublicKeys(signer),
                        },
                        Timeout: 10 * time.Second,
                        HostKeyCallback: hostKeyCallback,
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
		return nil, err
	}
	return ses, nil
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
