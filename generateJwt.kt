import java.security.spec.PKCS8EncodedKeySpec
import java.security.KeyFactory
import java.util.Base64
import java.util.Date
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.SignatureAlgorithm

fun generateJwt(appId: String, privateKey: String): String {
    val pkcs8KeySpec = PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKey))
    val key = KeyFactory.getInstance("RSA").generatePrivate(pkcs8KeySpec)
    return Jwts.builder()
        .setIssuer(appId)
        .setIssuedAt(Date())
        .setExpiration(Date(System.currentTimeMillis() + 600_000)) // 10 minutes
        .signWith(SignatureAlgorithm.RS256, key)
        .compact()
}
