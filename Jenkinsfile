class Globals {
    // constants
    static final String PROJECT = 'flexprep'
    static final String NEXUS_IMAGE_REPO_INTERN = 'docker-intern-nexus.meteoswiss.ch'
    static final String NEXUS_IMAGE_REPO_PUBLIC = 'docker-public-nexus.meteoswiss.ch'
    static final String NEXUS_IMAGE_NAME_INTERN = 'docker-intern-nexus.meteoswiss.ch/dispersionmodelling/flexpart-ifs/flexprep'
    static final String NEXUS_IMAGE_NAME_PUBLIC = 'docker-public-nexus.meteoswiss.ch/dispersionmodelling/flexpart-ifs/flexprep'
    static final String ECR_REPO = '493666016161.dkr.ecr.eu-central-2.amazonaws.com'
    static final String ECR_IMAGE_NAME = '493666016161.dkr.ecr.eu-central-2.amazonaws.com/dispersionmodelling/flexpart-ifs/flexprep'


    // sets the pipeline to execute all steps related to building the service
    static boolean build = false

    // sets to abort the pipeline if the Sonarqube QualityGate fails
    static boolean qualityGateAbortPipeline = false

    // sets the pipeline to execute all steps related to releasing the service
    static boolean release = false

    // sets the pipeline to execute all steps related to create and publish artifacts
    static boolean publishArtifacts = false

    // sets the pipeline to execute all steps related to deployment of the service
    static boolean deploy = false

    // sets the pipeline to execute all steps related to restart the service
    static boolean restart = false

    // sets the pipeline to execute all steps related to delete the service from the container platform
    static boolean deleteContainer = false

    // sets the pipeline to execute all steps related to trigger the trivy scan
    static boolean runTrivyScan = false

    // the project name in container platform
    static String cpProjectName = ''

    // Container deployment environment
    static String ocpEnv = ''

    // OpenShift cluster to deploy container (e.g. "api.cpdepl.meteoswiss.ch:6443"), empty to skip'
    static String ocpHostName = ''

    // the image tag used for tagging the image
    static String imageTagIntern = ''
    static String imageTagPublic = ''
    static String imageTagECR = ''

    // the service version
    static String version = ''

}


@Library('dev_tools@main') _
pipeline {
    agent { label 'podman' }

    parameters {
        choice(choices: ['Build', 'Publish Artifacts', 'Deploy', 'Release', 'Restart', 'Delete', 'Trivy-Scan'],
            description: 'Build type',
            name: 'buildChoice')

        choice(choices: ['flexpart-ifs-devt',
                         'flexpart-ifs-depl',
                         'flexpart-ifs-prod'],
               description: 'Environment',
               name: 'environment')

        string(name: 'version', description: 'The release version, must follow semantic versioning (e.g. v1.0.0)')
        booleanParam(name: 'PUBLISH_DOCUMENTATION', defaultValue: false, description: 'Publishes the generated documentation')
        booleanParam(name: 'PUSH_IMAGES_TO_ECR', defaultValue: false, description: 'Push images to ECR?')
    }

    options {
        // New jobs should wait until older jobs are finished
        disableConcurrentBuilds()
        // Discard old builds
        buildDiscarder(logRotator(artifactDaysToKeepStr: '7', artifactNumToKeepStr: '1', daysToKeepStr: '45', numToKeepStr: '10'))
        // Timeout the pipeline build after 1 hour
        timeout(time: 1, unit: 'HOURS')
        gitLabConnection('CollabGitLab')
    }

    environment {
        KUBECONFIG = "$workspace/.kube/config"
        scannerHome = tool name: 'Sonarqube-certs-PROD', type: 'hudson.plugins.sonar.SonarRunnerInstallation'
        PATH = "/opt/maker/tools/aws:$PATH"
        TAG = "${sh(script: "echo `date +%g%m.$GIT_COMMIT`", returnStdout: true).trim()}"
    }

    stages {
        stage('Preflight') {
            steps {
                updateGitlabCommitStatus name: 'Build', state: 'running'

                script {
                    echo 'Starting with Preflight'
                    Globals.ocpEnv = params.environment
                    def (deployComponent, deployOcpEnv) = params.environment.split('-')
                    def cpAPIEnv = deployOcpEnv == 'prod' ? 'cp' : 'cpnonprod'
                    Globals.ocpHostName = "https://api.${cpAPIEnv}.meteoswiss.ch:6443"
                    Globals.cpProjectName = "${deployComponent}-main-${deployOcpEnv}"
                    // Determine the type of build
                    switch (params.buildChoice) {
                        case 'Build':
                            Globals.build = true
                            break
                        case 'Publish Artifacts':
                            Globals.publishArtifacts = true
                            break
                        case 'Deploy':
                            Globals.deploy = true
                            break
                        case 'Release':
                            Globals.release = true
                            break
                        case 'Restart':
                            Globals.restart = true
                            break
                        case 'Delete':
                            Globals.deleteContainer = true
                            break
                        case 'Trivy-Scan':
                            Globals.runTrivyScan = true
                            break
                    }

                    if (Globals.release) {
                        echo 'Starting with Release'
                        runDevScript("build/pymch-release.sh ${params.version}")
                    }

                    if (Globals.build || Globals.publishArtifacts || Globals.deploy || Globals.runTrivyScan) {
                        echo 'Starting with calulating version'
                        def shortBranchName = env.BRANCH_NAME.replaceAll("[^a-zA-Z0-9]+", "").take(30).toLowerCase()
                        try {
                            Globals.version = sh(script: "git describe --tags --match v[0-9]*", returnStdout: true).trim()
                        } catch (err) {
                            def version = sh(script: "git rev-parse --short HEAD", returnStdout: true).trim()
                            Globals.version = "${shortBranchName}-${version}"
                        }
                        echo "Using version ${Globals.version}"
                        Globals.imageTagIntern = "${Globals.NEXUS_IMAGE_NAME_INTERN}:${TAG}"
                        Globals.imageTagPublic = "${Globals.NEXUS_IMAGE_NAME_PUBLIC}:${TAG}"
                        Globals.imageTagECR = "${Globals.ECR_IMAGE_NAME}:${TAG}"
                        echo "Using container version ${Globals.imageTagIntern}, ${Globals.imageTagPublic} and ${Globals.imageTagECR} "
                    }
                }
            }
        }


        stage('Build') {
            when { expression { Globals.build } }
            steps {
                echo "Starting with Build image"
                sh """
                podman build --pull --build-arg VERSION=${Globals.version} --target tester -t ${Globals.imageTagIntern}-tester .
                mkdir -p test_reports
                """
                echo "Starting with unit-testing including coverage"
                sh "podman run --rm -v \$(pwd)/test_reports:/src/test_reports ${Globals.imageTagIntern}-tester sh -c '. ./test_ci.sh && run_tests_with_coverage'"

            }
            post {
                always {
                    junit keepLongStdio: true, testResults: 'test_reports/junit.xml'
                }
            }
        }


        stage('Scan') {
            when { expression { Globals.build } }
            steps {
                script {
                    echo("---- LYNT ----")
                    sh "podman run --rm -v \$(pwd)/test_reports:/src/test_reports ${Globals.imageTagIntern}-tester sh -c '. ./test_ci.sh && run_pylint'"

                    try {
                        echo("---- TYPING CHECK ----")
                        sh "podman run --rm -v \$(pwd)/test_reports:/src/test_reports ${Globals.imageTagIntern}-tester sh -c '. ./test_ci.sh && run_mypy'"
                        recordIssues(qualityGates: [[threshold: 10, type: 'TOTAL', unstable: false]], tools: [myPy(pattern: 'test_reports/mypy.log')])
                    }
                    catch (err) {
                        error "Too many mypy issues, exiting now..."
                    }

                    echo("---- SONARQUBE ANALYSIS ----")
                    withSonarQubeEnv("Sonarqube-PROD") {
                        // fix source path in coverage.xml
                        // (required because coverage is calculated using podman which uses a differing file structure)
                        // https://stackoverflow.com/questions/57220171/sonarqube-client-fails-to-parse-pytest-coverage-results
                        sh "sed -i 's/\\/src\\/app-root/.\\//g' test_reports/coverage.xml"
                        sh "${scannerHome}/bin/sonar-scanner"
                    }

                    echo("---- SONARQUBE QUALITY GATE ----")
                    timeout(time: 1, unit: 'HOURS') {
                        // Parameter indicates whether to set pipeline to UNSTABLE if Quality Gate fails
                        // true = set pipeline to UNSTABLE, false = don't
                        waitForQualityGate abortPipeline: Globals.qualityGateAbortPipeline
                    }
                }
            }
        }

        stage('Create Artifacts') {
            when { expression { Globals.build || Globals.publishArtifacts || Globals.deploy || params.PUBLISH_DOCUMENTATION } }
            steps {
                script {
                    if (expression { Globals.build || Globals.publishArtifacts ||  Globals.deploy }) {
                        echo "---- CREATE IMAGE ----"
                        sh """
                        podman build --pull --target runner --build-arg VERSION=${Globals.version} -t ${Globals.imageTagIntern} .
                        podman tag ${Globals.imageTagIntern} ${Globals.imageTagPublic} ${Globals.imageTagECR}
                        """
                    }
                }
                script {
                    if (params.PUBLISH_DOCUMENTATION) {
                        echo "---- CREATE DOCUMENTATION ----"
                        sh """
                        podman build --pull --build-arg VERSION=${Globals.version} --target documenter -t ${Globals.imageTagIntern}-documenter .
                        mkdir -p doc/_build
                        podman run -v \$(pwd)/doc/_build:/src/doc/_build --rm ${Globals.imageTagIntern}-documenter
                        """
                    }
                }
            }
        }

        stage('Publish Artifacts') {
            when { expression { Globals.publishArtifacts || Globals.deploy || params.PUBLISH_DOCUMENTATION } }
            environment {
                PATH = "$HOME/tools/openshift-client-tools:$PATH"
            }
            steps {
                script {
                    if (expression { Globals.publishArtifacts || Globals.deploy }) {
                        echo "---- PUBLISH IMAGE ----"
                        withCredentials([usernamePassword(credentialsId: 'openshift-nexus',
                            passwordVariable: 'NXPASS', usernameVariable: 'NXUSER')]) {
                            sh """
                            echo $NXPASS | podman login ${Globals.NEXUS_IMAGE_REPO_INTERN} -u $NXUSER --password-stdin
                            podman push ${Globals.imageTagIntern}
                            echo $NXPASS | podman login ${Globals.NEXUS_IMAGE_REPO_PUBLIC} -u $NXUSER --password-stdin
                            podman push ${Globals.imageTagPublic}
                            """
                        }
                        if (params.PUSH_IMAGES_TO_ECR) {
                            withCredentials([usernamePassword(credentialsId: 'aws-icon-sandbox',
                                                        passwordVariable: 'AWS_SECRET_ACCESS_KEY',
                                                        usernameVariable: 'AWS_ACCESS_KEY_ID')]) {
                            echo "---- PUBLISH IMAGE TO ECR ----"
                            sh """
                            #!/bin/bash

                            if test -f /etc/ssl/certs/ca-certificates.crt; then
                                export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
                            else
                                export AWS_CA_BUNDLE=/etc/ssl/certs/ca-bundle.crt
                            fi
                            aws ecr get-login-password --region eu-central-2 | podman login --username AWS --password-stdin --cert-dir /etc/ssl/certs ${Globals.ECR_REPO}

                            podman push --cert-dir /etc/ssl/certs ${Globals.imageTagECR}
                            aws ssm put-parameter --name "/flexpart-ifs/flexprep/containertag" --type "String" --value "${TAG}" --overwrite --region eu-central-2

                            """
                            }
                        }
                    }
                }
                script {
                    if (params.PUBLISH_DOCUMENTATION) {
                        echo "---- PUBLISH DOCUMENTATION ----"
                        withCredentials([string(credentialsId: "documentation-main-prod-token", variable: 'TOKEN')]) {
                            sh "oc login https://api.cp.meteoswiss.ch:6443 --token \$TOKEN"
                            script {
                                publishDoc 'doc/_build/', Globals.PROJECT, Globals.version, 'python'
                            }
                        }
                    }
                }
            }
            post {
                cleanup {
                    script {
                        sh "podman logout ${Globals.NEXUS_IMAGE_REPO_INTERN} || true"
                        if (env.BRANCH_NAME == 'main'){
                            sh "podman logout ${Globals.NEXUS_IMAGE_REPO_PUBLIC} || true"
                        }
                        if (params.PUSH_IMAGES_TO_ECR) {
                            sh "podman logout ${Globals.ECR_REPO} || true"
                        }
                        sh 'oc logout || true'
                    }
                }
            }
        }


        stage('Scan Artifacts') {
            when { expression { Globals.runTrivyScan } }
            environment {
                PATH = "$HOME/tools/openshift-client-tools:$HOME/tools/trivy:$PATH"
                HTTP_PROXY = "http://proxy.meteoswiss.ch:8080"
                HTTPS_PROXY = "http://proxy.meteoswiss.ch:8080"
            }
            steps {
                script {
                    echo "---- TRIVY SCAN ----"
                    withCredentials([usernamePassword(credentialsId: 'openshift-nexus',
                        passwordVariable: 'NXPASS', usernameVariable: 'NXUSER')]) {
                        sh "echo $NXPASS | podman login ${Globals.NEXUS_IMAGE_REPO_INTERN} -u $NXUSER --password-stdin"
                        runDevScript("test/trivyscanner.py ${Globals.imageTagIntern}")
                        if (env.BRANCH_NAME == 'main'){
                            sh "echo $NXPASS | podman login ${Globals.NEXUS_IMAGE_REPO_PUBLIC} -u $NXUSER --password-stdin"
                            runDevScript("test/trivyscanner.py ${Globals.imageTagPublic}")
                        }
                    }
                }
            }
            post {
                cleanup {
                    script {
                        sh "podman logout ${Globals.NEXUS_IMAGE_REPO_INTERN} || true"
                        if (env.BRANCH_NAME == 'main'){
                            sh "podman logout ${Globals.NEXUS_IMAGE_REPO_PUBLIC} || true"
                        }
                    }
                }
            }
        }

        stage('Deploy') {
            when { expression { Globals.deploy } }
            environment {
                PATH = "$HOME/tools/openshift-client-tools:$PATH"
            }
            steps {
                script {
                    // we tag the current commit as the one deployed to the target environment
                    sh """
                    git tag -f ${Globals.ocpEnv}
                    git push -f origin ${Globals.ocpEnv}
                    """

                    withCredentials([usernamePassword(credentialsId: 'openshift-nexus',
                        passwordVariable: 'NXPASS', usernameVariable: 'NXUSER')]) {
                        echo 'Push to image registry'
                        sh """
                           echo $NXPASS | podman login ${Globals.NEXUS_IMAGE_REPO_INTERN} -u $NXUSER --password-stdin
                           podman tag ${Globals.imageTagIntern} ${Globals.NEXUS_IMAGE_NAME_INTERN}:${Globals.ocpEnv}
                           podman push ${Globals.NEXUS_IMAGE_NAME_INTERN}:${Globals.ocpEnv}
                        """
                    }

                    withCredentials([string(credentialsId: "${Globals.cpProjectName}-token", variable: 'TOKEN')]) {
                        sh "oc login ${Globals.ocpHostName} --token $TOKEN"
                        sh "oc project ${Globals.cpProjectName}"
                        sh "kubectl apply -k k8s/overlays/${Globals.ocpEnv}"
                        sh "kubectl rollout restart deployment/flexprep"
                    }
                }
            }
            post {
                cleanup {
                    sh "podman logout ${Globals.NEXUS_IMAGE_REPO_INTERN} || true"
                    sh "oc logout || true"
                }
            }
        }

        stage('Restart Deployment') {
            when { expression { Globals.restart } }
            environment {
                PATH = "$HOME/tools/openshift-client-tools:$PATH"
            }
            steps {
                withCredentials([string(credentialsId: "${Globals.cpProjectName}-token", variable: 'TOKEN')]) {
                    sh "oc login ${Globals.ocpHostName} --token \$TOKEN"
                    sh "oc project ${Globals.cpProjectName}"
                    sh "kubectl rollout restart deployment/flexprep"
                }
            }
            post {
                cleanup {
                    sh "oc logout || true"
                }
            }
        }

        stage('Delete Deployment') {
            when { expression { Globals.deleteContainer } }
            environment {
                PATH = "$HOME/tools/openshift-client-tools:$PATH"
            }
            steps {
                withCredentials([string(credentialsId: "${Globals.cpProjectName}-token", variable: 'TOKEN')]) {
                    sh """
                    oc login ${Globals.ocpHostName} --token \$TOKEN
                    oc project ${Globals.cpProjectName}
                    kubectl delete -k k8s/overlays/${Globals.ocpEnv}
                    """
                }
            }
            post {
                cleanup {
                    sh """
                    oc logout || true
                    """
                }
            }
        }
    }


    post {
        cleanup {
            sh "podman image rm -f ${Globals.imageTagIntern}-documenter || true"
            sh "podman image rm -f ${Globals.imageTagIntern}-tester || true"
            sh "podman image rm -f ${Globals.imageTagIntern} || true"
            sh "podman image rm -f ${Globals.imageTagPublic} || true"
            sh "podman image rm -f ${Globals.imageTagECR} || true"
            sh "podman image rm -f ${Globals.NEXUS_IMAGE_NAME_INTERN}:${Globals.ocpEnv} || true"
        }
        aborted {
            updateGitlabCommitStatus name: 'Build', state: 'canceled'
        }
        failure {
            updateGitlabCommitStatus name: 'Build', state: 'failed'
            echo 'Sending email'
            sh 'df -h'
            emailext(subject: "${currentBuild.fullDisplayName}: ${currentBuild.currentResult}",
                attachLog: true,
                attachmentsPattern: 'generatedFile.txt',
                body: "Job '${env.JOB_NAME} #${env.BUILD_NUMBER}': ${env.BUILD_URL}",
                recipientProviders: [requestor(), developers()])
        }
        success {
            echo 'Build succeeded'
            updateGitlabCommitStatus name: 'Build', state: 'success'
        }
    }
}
