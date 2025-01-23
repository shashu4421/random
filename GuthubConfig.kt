import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.cloud.config.server.config.ConfigServerProperties

@Configuration
class GitHubConfig {

    @Value("\${github.app-id}")
    lateinit var appId: String

    @Value("\${github.private-key}")
    lateinit var privateKey: String

    @Value("\${github.installation-id}")
    lateinit var installationId: String

    @Bean
    fun configureGit(): ConfigServerProperties {
        val jwt = generateJwt(appId, privateKey)
        val token = fetchAccessToken(jwt, installationId)

        val properties = ConfigServerProperties()
        properties.setGitUri("https://github.com/your-org/your-repo")
        properties.setGitUsername("x-access-token")
        properties.setGitPassword(token)
        return properties
    }
}
