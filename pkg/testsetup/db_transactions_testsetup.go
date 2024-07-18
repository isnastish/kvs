package testsetup

var postgresDockerImage = "postgres:16.3"

func StartPostgresContainer() (bool, error) {
	expectedOutput := "PostgreSQL init process complete; ready for start up"
	return startDockerContainer(
		expectedOutput,
		"docker", "run", "--rm", "--name", "postgres-emulator", "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=nastish", postgresDockerImage)
}

func KillPostgresContainer() {
	killDockerContainer("docker", "rm", "--force", "postgres-emulator")
}