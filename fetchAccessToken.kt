import org.springframework.web.client.RestTemplate
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpEntity
import org.springframework.http.HttpMethod

fun fetchAccessToken(jwt: String, installationId: String): String {
    val url = "https://api.github.com/app/installations/$installationId/access_tokens"
    val headers = HttpHeaders()
    headers.set("Authorization", "Bearer $jwt")
    headers.set("Accept", "application/vnd.github.v3+json")

    val entity = HttpEntity(null, headers)
    val restTemplate = RestTemplate()
    val response = restTemplate.exchange(url, HttpMethod.POST, entity, Map::class.java)

    return response.body?.get("token").toString()
}
