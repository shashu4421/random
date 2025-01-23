import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import java.util.Date

fun getPrivateKeyFromPem(pemKey: String): RSAPrivateKey {
    // Remove PEM header and footer
    val key = pemKey
        .replace("-----BEGIN PRIVATE KEY-----", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replace("\\s".toRegex(), "") // Remove all whitespace

    // Decode Base64 to get the binary key
    val keyBytes = Base64.getDecoder().decode(key)

    // Create the private key object
    val keySpec = PKCS8EncodedKeySpec(keyBytes)
    val keyFactory = KeyFactory.getInstance("RSA")
    return keyFactory.generatePrivate(keySpec) as RSAPrivateKey
}

fun generateJwtWithAuth0(appId: String, pemPrivateKey: String): String {
    val privateKey = getPrivateKeyFromPem(pemPrivateKey)
    val algorithm = Algorithm.RSA256(null, privateKey)

    return JWT.create()
        .withIssuer(appId)
        .withIssuedAt(Date())
        .withExpiresAt(Date(System.currentTimeMillis() + 600_000)) // 10 minutes
        .sign(algorithm)
}

