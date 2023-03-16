/*
 *  Copyright (c) 2022 Gregor Münker
 *
 */

plugins {
    `java-library`
}

dependencies {
    api(edc.spi.ids)
    api(edc.spi.contract)
    api(edc.core.connector)

    api("org.eclipse.edc:control-plane-spi:0.0.1-milestone-8")
    api("org.eclipse.edc:data-plane-spi:0.0.1-milestone-8")
    implementation("org.eclipse.edc:control-plane-core:0.0.1-milestone-8")
    implementation("org.eclipse.edc:data-plane-core:0.0.1-milestone-8")
    implementation("org.eclipse.edc:data-plane-util:0.0.1-milestone-8")
    implementation("org.eclipse.edc:data-plane-client:0.0.1-milestone-8")
    implementation("org.eclipse.edc:data-plane-selector-client:0.0.1-milestone-8")
    implementation("org.eclipse.edc:data-plane-selector-core:0.0.1-milestone-8")
    implementation("org.eclipse.edc:transfer-data-plane:0.0.1-milestone-8")
    implementation("org.eclipse.edc:transfer-spi:0.0.1-milestone-8")
    implementation(libs.opentelemetry.annotations)
    implementation("org.json:json:20230227")

    implementation(edc.ext.azure.blob.core)
    implementation(libs.azure.storageblob)
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
