import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.nio.file.Files
import java.nio.file.Paths

import java.security.KeyFactory
import java.security.PrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

@Configuration
class TokenConfiguration {

    @Bean
    fun setGitPassword(): ApplicationRunner {
        return ApplicationRunner {
            val token = generateGitHubToken()
            System.setProperty("GIT_PASSWORD", token)
        }
    }

    private fun generateGitHubToken(): String {
        // Example: Generate a GitHub token using a private key and JWT
        val privateKeyPath = Paths.get("path/to/your/private-key.pem")
        val privateKey = Files.readString(privateKeyPath)
        val jwt = createJWT(appId = "your-app-id", privateKey = privateKey)
        return fetchInstallationToken(jwt, installationId = "your-installation-id")
    }

    private fun createJWT(appId: String, privateKey: String): String {
        // Use a library like java-jwt to create the JWT
        val algorithm = com.auth0.jwt.algorithms.Algorithm.RSA256(null, privateKey.toPrivateKey())
        return com.auth0.jwt.JWT.create()
            .withIssuer(appId)
            .withIssuedAt(Date.from(Instant.now()))
            .withExpiresAt(Date.from(Instant.now().plusSeconds(600))) // 10 minutes
            .sign(algorithm)
    }

    private fun fetchInstallationToken(jwt: String, installationId: String): String {
        val webClient = org.springframework.web.reactive.function.client.WebClient.builder()
            .baseUrl("https://api.github.com")
            .defaultHeader("Authorization", "Bearer $jwt")
            .defaultHeader("Accept", "application/vnd.github.v3+json")
            .build()

        val response = webClient.post()
            .uri("/app/installations/$installationId/access_tokens")
            .retrieve()
            .bodyToMono(Map::class.java)
            .block()

        return response?.get("token") as String
    }

fun String.toPrivateKey(): PrivateKey {
    // Remove PEM headers and newlines
    val privateKeyContent = this
        .replace("-----BEGIN PRIVATE KEY-----", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replace("\\s".toRegex(), "")

    // Decode Base64
    val keyBytes = Base64.getDecoder().decode(privateKeyContent)

    // Convert to PrivateKey object
    val keySpec = PKCS8EncodedKeySpec(keyBytes)
    val keyFactory = KeyFactory.getInstance("RSA") // Replace "RSA" with the correct algorithm if needed
    return keyFactory.generatePrivate(keySpec)
}
}
