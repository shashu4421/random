import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import java.util.Date

fun generateJwtWithAuth0(appId: String, privateKey: String): String {
    val algorithm = Algorithm.RSA256(null, privateKey.toByteArray())
    return JWT.create()
        .withIssuer(appId)
        .withIssuedAt(Date())
        .withExpiresAt(Date(System.currentTimeMillis() + 600_000)) // 10 minutes
        .sign(algorithm)
}
