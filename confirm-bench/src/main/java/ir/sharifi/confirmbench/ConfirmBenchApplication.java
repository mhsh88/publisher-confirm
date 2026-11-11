package ir.sharifi.confirmbench;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class ConfirmBenchApplication {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(ConfirmBenchApplication.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		app.setBannerMode(Banner.Mode.OFF);
		ConfigurableApplicationContext context = app.run(args);
		System.exit(SpringApplication.exit(context));
	}
}
