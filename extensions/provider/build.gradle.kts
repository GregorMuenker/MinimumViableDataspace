/*
 * Gregor Münker SAP SE
 */

plugins {
    `java-library`
}

dependencies {
    api(edc.spi.ids)
    api(edc.spi.contract)
    api(edc.core.connector)
    implementation(identityHub.spi.core)
    implementation(fcc.spi)
    implementation(edc.spi.ids)
    implementation(edc.util)
    implementation(edc.identity.did.core)
    implementation(edc.identity.did.web)
    
    implementation(libs.jakarta.rsApi)
    implementation("org.eclipse.edc:http:0.0.1-milestone-8")

    testImplementation(edc.policy.engine)
}
