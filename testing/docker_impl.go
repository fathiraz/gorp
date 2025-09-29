// Docker container management implementation
package testing

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// dockerContainerOptions contains options for starting a Docker container
type dockerContainerOptions struct {
	Name        string
	Image       string
	Ports       map[string]string // host:container
	Env         []string
	HealthCheck func() error
	Timeout     time.Duration
}

// isDockerAvailable checks if Docker is available and running
func isDockerAvailable() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// startDockerContainer starts a Docker container with the given options
func startDockerContainer(opts dockerContainerOptions) (string, error) {
	// Build docker run command
	args := []string{"run", "-d", "--rm", "--name", opts.Name}

	// Add port mappings
	for hostPort, containerPort := range opts.Ports {
		args = append(args, "-p", fmt.Sprintf("%s:%s", hostPort, containerPort))
	}

	// Add environment variables
	for _, env := range opts.Env {
		args = append(args, "-e", env)
	}

	// Add image
	args = append(args, opts.Image)

	// Start container
	cmd := exec.Command("docker", args...)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to start container: %w", err)
	}

	containerID := strings.TrimSpace(string(output))

	// Wait for container to be healthy
	if opts.HealthCheck != nil {
		ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
		defer cancel()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				stopDockerContainer(containerID)
				return "", fmt.Errorf("container health check timeout")
			case <-ticker.C:
				if err := opts.HealthCheck(); err == nil {
					return containerID, nil
				}
			}
		}
	}

	return containerID, nil
}

// stopDockerContainer stops and removes a Docker container
func stopDockerContainer(containerID string) error {
	cmd := exec.Command("docker", "stop", containerID)
	if err := cmd.Run(); err != nil {
		// If container is already stopped, try to remove it anyway
		removeCmd := exec.Command("docker", "rm", containerID)
		removeCmd.Run()
		return fmt.Errorf("failed to stop container %s: %w", containerID, err)
	}
	return nil
}

// getContainerLogs returns logs from a Docker container
func getContainerLogs(containerID string) (string, error) {
	cmd := exec.Command("docker", "logs", containerID)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get logs for container %s: %w", containerID, err)
	}
	return string(output), nil
}

// execInContainer executes a command inside a Docker container
func execInContainer(containerID string, command ...string) (string, error) {
	args := append([]string{"exec", containerID}, command...)
	cmd := exec.Command("docker", args...)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute command in container %s: %w", containerID, err)
	}
	return string(output), nil
}

// inspectContainer returns detailed information about a Docker container
func inspectContainer(containerID string) (map[string]interface{}, error) {
	cmd := exec.Command("docker", "inspect", containerID)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container %s: %w", containerID, err)
	}

	// For simplicity, return a basic map. In production, this would parse JSON.
	result := make(map[string]interface{})
	result["output"] = string(output)
	return result, nil
}

// waitForPort waits for a port to become available on localhost
func waitForPort(port string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for port %s", port)
		case <-ticker.C:
			// Try to connect to the port
			cmd := exec.Command("nc", "-z", "localhost", port)
			if cmd.Run() == nil {
				return nil
			}
		}
	}
}

// DockerComposeManager manages Docker Compose for complex test scenarios
type DockerComposeManager struct {
	composeFile string
	projectName string
}

// NewDockerComposeManager creates a new Docker Compose manager
func NewDockerComposeManager(composeFile, projectName string) *DockerComposeManager {
	return &DockerComposeManager{
		composeFile: composeFile,
		projectName: projectName,
	}
}

// Up starts all services defined in the Docker Compose file
func (dcm *DockerComposeManager) Up() error {
	cmd := exec.Command("docker-compose",
		"-f", dcm.composeFile,
		"-p", dcm.projectName,
		"up", "-d")
	return cmd.Run()
}

// Down stops and removes all services
func (dcm *DockerComposeManager) Down() error {
	cmd := exec.Command("docker-compose",
		"-f", dcm.composeFile,
		"-p", dcm.projectName,
		"down", "-v")
	return cmd.Run()
}

// Logs returns logs from all services
func (dcm *DockerComposeManager) Logs() (string, error) {
	cmd := exec.Command("docker-compose",
		"-f", dcm.composeFile,
		"-p", dcm.projectName,
		"logs")
	output, err := cmd.Output()
	return string(output), err
}

// Scale scales a service to the specified number of instances
func (dcm *DockerComposeManager) Scale(service string, instances int) error {
	cmd := exec.Command("docker-compose",
		"-f", dcm.composeFile,
		"-p", dcm.projectName,
		"scale", fmt.Sprintf("%s=%d", service, instances))
	return cmd.Run()
}

// DockerRegistry manages a local Docker registry for testing
type DockerRegistry struct {
	containerID string
	port        string
}

// NewDockerRegistry starts a local Docker registry for testing
func NewDockerRegistry(port string) (*DockerRegistry, error) {
	containerID, err := startDockerContainer(dockerContainerOptions{
		Name:  fmt.Sprintf("test-registry-%d", time.Now().Unix()),
		Image: "registry:2",
		Ports: map[string]string{port: "5000"},
		HealthCheck: func() error {
			cmd := exec.Command("curl", "-f", fmt.Sprintf("http://localhost:%s/v2/", port))
			return cmd.Run()
		},
		Timeout: 30 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &DockerRegistry{
		containerID: containerID,
		port:        port,
	}, nil
}

// Close stops and removes the registry container
func (dr *DockerRegistry) Close() error {
	return stopDockerContainer(dr.containerID)
}

// Push pushes an image to the test registry
func (dr *DockerRegistry) Push(imageName string) error {
	registryImage := fmt.Sprintf("localhost:%s/%s", dr.port, imageName)

	// Tag the image
	tagCmd := exec.Command("docker", "tag", imageName, registryImage)
	if err := tagCmd.Run(); err != nil {
		return fmt.Errorf("failed to tag image: %w", err)
	}

	// Push the image
	pushCmd := exec.Command("docker", "push", registryImage)
	return pushCmd.Run()
}

// Pull pulls an image from the test registry
func (dr *DockerRegistry) Pull(imageName string) error {
	registryImage := fmt.Sprintf("localhost:%s/%s", dr.port, imageName)
	cmd := exec.Command("docker", "pull", registryImage)
	return cmd.Run()
}

// NetworkManager manages Docker networks for test isolation
type NetworkManager struct {
	networks map[string]string // name -> id
}

// NewNetworkManager creates a new network manager
func NewNetworkManager() *NetworkManager {
	return &NetworkManager{
		networks: make(map[string]string),
	}
}

// CreateNetwork creates a new Docker network
func (nm *NetworkManager) CreateNetwork(name string) error {
	cmd := exec.Command("docker", "network", "create", name)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to create network %s: %w", name, err)
	}

	networkID := strings.TrimSpace(string(output))
	nm.networks[name] = networkID
	return nil
}

// ConnectContainer connects a container to a network
func (nm *NetworkManager) ConnectContainer(networkName, containerID string) error {
	cmd := exec.Command("docker", "network", "connect", networkName, containerID)
	return cmd.Run()
}

// DisconnectContainer disconnects a container from a network
func (nm *NetworkManager) DisconnectContainer(networkName, containerID string) error {
	cmd := exec.Command("docker", "network", "disconnect", networkName, containerID)
	return cmd.Run()
}

// RemoveNetwork removes a Docker network
func (nm *NetworkManager) RemoveNetwork(name string) error {
	cmd := exec.Command("docker", "network", "rm", name)
	err := cmd.Run()
	if err == nil {
		delete(nm.networks, name)
	}
	return err
}

// CleanupAll removes all managed networks
func (nm *NetworkManager) CleanupAll() error {
	var errors []error
	for name := range nm.networks {
		if err := nm.RemoveNetwork(name); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("network cleanup errors: %v", errors)
	}
	return nil
}

// VolumeManager manages Docker volumes for test data persistence
type VolumeManager struct {
	volumes map[string]string // name -> id
}

// NewVolumeManager creates a new volume manager
func NewVolumeManager() *VolumeManager {
	return &VolumeManager{
		volumes: make(map[string]string),
	}
}

// CreateVolume creates a new Docker volume
func (vm *VolumeManager) CreateVolume(name string) error {
	cmd := exec.Command("docker", "volume", "create", name)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to create volume %s: %w", name, err)
	}

	volumeID := strings.TrimSpace(string(output))
	vm.volumes[name] = volumeID
	return nil
}

// RemoveVolume removes a Docker volume
func (vm *VolumeManager) RemoveVolume(name string) error {
	cmd := exec.Command("docker", "volume", "rm", name)
	err := cmd.Run()
	if err == nil {
		delete(vm.volumes, name)
	}
	return err
}

// CleanupAll removes all managed volumes
func (vm *VolumeManager) CleanupAll() error {
	var errors []error
	for name := range vm.volumes {
		if err := vm.RemoveVolume(name); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("volume cleanup errors: %v", errors)
	}
	return nil
}

// streamLogs streams logs from a container
func streamLogs(containerID string) (<-chan string, error) {
	cmd := exec.Command("docker", "logs", "-f", containerID)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	logChan := make(chan string, 100)
	go func() {
		defer close(logChan)
		defer cmd.Wait()

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			logChan <- scanner.Text()
		}
	}()

	return logChan, nil
}