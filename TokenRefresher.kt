import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class TokenRefresher {

    @Scheduled(fixedRate = 9 * 60 * 1000) // Refresh every 9 minutes
    fun refreshToken() {
        val token = generateGitHubToken() // Same method as above
        System.setProperty("GIT_PASSWORD", token)
    }
}
