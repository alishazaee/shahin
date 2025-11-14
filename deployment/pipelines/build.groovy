pipeline {
    agent any
    options {
        disableConcurrentBuilds()
        timestamps()
    }

    environment {
        NEXUS_URL = 'alishazaei'
        MODULES = 'ingest'
        NEXUS_USER = 'admin'
        NEXUS_PASSWORD = credentials('nexus_password')
    }
    stages {
        stage('List Workspace') {
            steps {
                script {
                    echo "Printing workspace at root"
                    sh "ls -alh"
                }
            }
        }

        stage('Build Docker Images') {
            steps {
                script {

                    def modules = env.MODULES.split(',')
                    for (module in modules) {
                            sh """
                         ./gradlew :${module}:jibDockerBuild  -DNEXUS_URL=${NEXUS_URL}  --no-configuration-cache
                                """
                    }
                }
            }
        }

        stage('Push Docker Images to Nexus') {
            steps {
                script {

                    sh """
                     docker login -u ${NEXUS_USER} -p ${NEXUS_PASSWORD} http://${NEXUS_URL}
                       """

                    def modules = env.MODULES.split(',')
                    for (module in modules) {
                        def version = sh(returnStdout: true, script: "./gradlew -q :${module}:printVersion")
                        sh "docker push ${NEXUS_URL}/${module}:${version}"
                    }

                }
            }
        }
    }
}