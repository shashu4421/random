import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.springframework.cloud.config.server.environment.JGitEnvironmentProperties
import org.springframework.cloud.config.server.environment.JGitEnvironmentRepository
import org.springframework.stereotype.Component

@Component
class DynamicGitHubRepository(
    properties: JGitEnvironmentProperties,
    private val tokenService: GitHubTokenService
) : JGitEnvironmentRepository(properties) {

    override fun getCredentialsProvider(): UsernamePasswordCredentialsProvider {
        val installationToken = tokenService.getNewInstallationToken()
        return UsernamePasswordCredentialsProvider("x-access-token", installationToken)
    }
}
