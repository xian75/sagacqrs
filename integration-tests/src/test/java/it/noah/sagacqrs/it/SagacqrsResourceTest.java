package it.noah.sagacqrs.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class SagacqrsResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/sagacqrs")
                .then()
                .statusCode(200)
                .body(is("Hello sagacqrs"));
    }
}
