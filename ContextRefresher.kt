import org.springframework.context.ConfigurableApplicationContext
import org.springframework.stereotype.Component

@Component
class ContextRefresher(
    private val context: ConfigurableApplicationContext
) {
    fun refreshContext() {
        context.refresh()
    }
}

