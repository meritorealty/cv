<?php

namespace Manzano\CvdwCli\Services;

use Manzano\CvdwCli\Services\Console\CvdwSymfonyStyle;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class Http
{
    protected CvdwSymfonyStyle $console;
    public InputInterface $input;
    public OutputInterface $output;
    public $logObjeto;
    protected $evento = 'Requisição';
    public $executarObj;
    public $ratelimitObj;
    public $tempodeexecucao = 0;
    protected EnvironmentManager $environmentManager;
    public const HEADER_CONTENT_TYPE = 'Content-Type: application/json';
    public const ERRO_REQUISICAO = 'Erro ao tentar fazer a requisição!';
    public const PROTOCOLO_HTTP = 'https://';
    public const RESPONSE_TOO_MANY_REQUESTS = 'Too many requests';
    public const ERRO_BLOQUEIO = 'O servidor bloqueia o acesso ao CVDW se forem feitas mais que 20 requisições por minuto.';
    public const TENTAR_NOVAMENTE = 'Você pode tentar novamente dentro de um minuto...';

    public function __construct(
        InputInterface $input,
        OutputInterface $output,
        CvdwSymfonyStyle $console,
        $executarObj,
        $logObjeto,
        RateLimit $rateLimitObj
    ) {
        if (is_object($logObjeto)) {
            $this->logObjeto = $logObjeto;
        }
        $this->console = $console;
        $this->input = $input;
        $this->output = $output;
        $this->executarObj = $executarObj;
        $this->ratelimitObj = $rateLimitObj;
        $this->environmentManager = new EnvironmentManager();
    }

    public function requestCVDW(string $path, $progressBar, $cvdw, array $parametros = [], bool $novaTentativa = true)
    {
        $this->ratelimitObj->validarTempoExecucao();

        // NOVO: Circuit breaker — se aberto, retorna null para pular
        if ($this->ratelimitObj->circuitoEstaAberto()) {
            $this->console->warning([
                'Circuit breaker ABERTO — pulando requisição para ' . $path,
                'Status: ' . $this->ratelimitObj->getCircuitBreakerStatus(),
            ]);
            return null;
        }

        // NOVO: Sliding window + intervalo mínimo (substitui gerenciarRateLimit antigo)
        $this->ratelimitObj->aguardarSeNecessario();

        // Exibir info de rate limit na progress bar (mantém UX original)
        if ($progressBar) {
            $this->tempodeexecucao = $this->ratelimitObj->tempoDeExecucao();
            $mensagem = "\n <fg=blue>Tempo de execução: " . $this->tempodeexecucao . " segundos</>";
            $mensagem .= "\n <fg=gray>Rate limiter ativo (18req/min, intervalo 3.5s)</>";
            $mensagem .= "\n <fg=gray>CB: " . $this->ratelimitObj->getCircuitBreakerStatus() . "</>";
            $mensagem = $cvdw->getMensagem($mensagem);
            $progressBar->setMessage($mensagem);
            $progressBar->display();
        }

        $idrequisicao = $this->ratelimitObj->inserirRequisicao($path);
        $this->ratelimitObj->registrarRequisicaoMemoria();

        $cabecalho = [
            'email: ' . $this->environmentManager->getCvEmail() . '',
            'token: ' . $this->environmentManager->getCvToken() . '',
            $this::HEADER_CONTENT_TYPE,
        ];

        $url = $this::PROTOCOLO_HTTP . $this->environmentManager->getCvUrl() . '.cvcrm.com.br/api/v1/cvdw' . $path;

        // ============================================================
        // NOVO: Loop de retry com backoff exponencial
        // ============================================================
        $maxRetries = 5;
        $tentativa = 0;
        $httpCode = 0;
        $response = '';

        while ($tentativa <= $maxRetries) {
            $curl = curl_init();
            $verbose = fopen('php://temp', 'w+');
            curl_setopt_array(
                $curl,
                [
                    CURLOPT_URL => $url,
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_ENCODING => '',
                    CURLOPT_MAXREDIRS => 10,
                    CURLOPT_CONNECTTIMEOUT => 10,
                    CURLOPT_TIMEOUT => 40,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                    CURLOPT_CUSTOMREQUEST => 'GET',
                    CURLOPT_POSTFIELDS => json_encode($parametros),
                    CURLOPT_HTTPHEADER => $cabecalho,
                    CURLOPT_SSL_VERIFYPEER => false,
                    CURLOPT_SSL_VERIFYHOST => false,
                    CURLOPT_VERBOSE => true,
                    CURLOPT_STDERR => $verbose,
                ]
            );

            $response = curl_exec($curl);
            $httpCode = 0;

            if (!curl_errno($curl)) {
                $httpCode = intval(curl_getinfo($curl, CURLINFO_HTTP_CODE));
            }

            curl_close($curl);

            $responseJson = json_decode($response);

            // SUCESSO (2xx)
            if ($httpCode >= 200 && $httpCode <= 299) {
                $this->ratelimitObj->registrarSucesso();
                if (isset($responseJson->dados)) {
                    $dadosRetornoQtd = count($responseJson->dados);
                } else {
                    $dadosRetornoQtd = null;
                }
                $this->ratelimitObj->concluirRequisicao($idrequisicao, $dadosRetornoQtd, $httpCode);
                return $responseJson;
            }

            // Debug verbose
            if ($this->output->isDebug() || $this->input->getOption('verbose')) {
                rewind($verbose);
                $verboseLog = stream_get_contents($verbose);
                $this->console->info([
                    'URL: ' . $url,
                    'HTTP Code: ' . $httpCode,
                    'Tentativa: ' . ($tentativa + 1) . '/' . ($maxRetries + 1),
                    'Resposta: ' . $response,
                ]);
            }

            // =====================================================
            // HTTP 429 — Too Many Requests (CORREÇÃO PRINCIPAL)
            // =====================================================
            if ($httpCode === 429 || (isset($responseJson->Response) && $responseJson->Response == $this::RESPONSE_TOO_MANY_REQUESTS)) {

                $tentativa++;

                // Backoff exponencial: 60s, 120s, 240s... (cap 300s)
                $baseWait = 60; // MÍNIMO 60s — bloqueio documentado da API
                $waitTime = $baseWait * pow(2, $tentativa - 1);
                $jitter = $waitTime * 0.15 * (mt_rand() / mt_getrandmax() * 2 - 1);
                $waitTime = (int) min($waitTime + $jitter, 300);

                $this->console->warning([
                    "HTTP 429 — Rate limit excedido (tentativa {$tentativa}/{$maxRetries})",
                    "Aguardando {$waitTime}s antes de tentar novamente...",
                    "CB: " . $this->ratelimitObj->getCircuitBreakerStatus(),
                ]);

                // Registrar falha no circuit breaker
                $podeContinuar = $this->ratelimitObj->registrarFalha429();
                if (!$podeContinuar) {
                    $this->console->error([
                        'Circuit breaker ABERTO após ' . $tentativa . ' falhas 429 consecutivas.',
                        'Pulando para o próximo endpoint. Recuperação em 2 minutos.',
                    ]);
                    $this->ratelimitObj->concluirRequisicao($idrequisicao, null, $httpCode);
                    return null;
                }

                // Aguardar com countdown na progress bar
                if ($progressBar) {
                    for ($i = $waitTime; $i > 0; $i--) {
                        $msg = " <fg=red>429 — Aguardando {$i}s (tentativa {$tentativa}/{$maxRetries})...</>";
                        $msg = $cvdw->getMensagem($msg);
                        $progressBar->setMessage($msg);
                        $progressBar->display();
                        sleep(1);
                    }
                } else {
                    sleep($waitTime);
                }

                // Registrar nova requisição no sliding window antes de retentar
                $this->ratelimitObj->aguardarSeNecessario();
                $this->ratelimitObj->registrarRequisicaoMemoria();

                continue; // Volta pro while
            }

            // =====================================================
            // HTTP 405 — Method Not Allowed (endpoint depreciado)
            // =====================================================
            if ($httpCode === 405) {
                $this->console->error([
                    "HTTP 405 — Endpoint possivelmente depreciado: {$path}",
                    "Verifique se a URL usa /api/v1/ (não /api/cvio/)",
                    "Pulando este endpoint.",
                ]);
                $this->ratelimitObj->concluirRequisicao($idrequisicao, null, $httpCode);
                return null;
            }

            // =====================================================
            // Outros erros — retry genérico com backoff menor
            // =====================================================
            $tentativa++;
            if ($tentativa > $maxRetries) {
                break;
            }

            $waitTime = min(10 * $tentativa, 60);
            $this->console->error([
                $this::ERRO_REQUISICAO,
                "HTTP {$httpCode} — Tentativa {$tentativa}/{$maxRetries}",
                "Aguardando {$waitTime}s...",
            ]);
            sleep($waitTime);
        }

        // Esgotou todas as tentativas
        $this->console->error([
            $this::ERRO_REQUISICAO,
            "Todas as {$maxRetries} tentativas falharam para: {$path}",
            $response,
        ]);
        $this->ratelimitObj->concluirRequisicao($idrequisicao, null, $httpCode);

        return null;
    }

    /**
     * @deprecated Substituído por $this->ratelimitObj->aguardarSeNecessario()
     * Mantido para não quebrar eventuais chamadas externas.
     */
    public function gerenciarRateLimit($cvdw, $progressBar): int
    {
        $diferenca = $this->ratelimitObj->getDiferencaSegundosUltimaRequisicao();
        $requisicoes = $this->ratelimitObj->qtdRequisicoes(60);

        if ($this->output->isDebug() || $this->input->getOption('verbose')) {
            $this->console->info(" LOG: Diferença: $diferenca");
        }

        $this->tempodeexecucao = $this->ratelimitObj->tempoDeExecucao();

        if ($progressBar && $requisicoes > 1) {
            $mensagem = null;
            $mensagem = "\n <fg=blue>Tempo de execução: " . $this->tempodeexecucao . " segundos</>";
            $mensagem .= "\n <fg=blue>Você fez " . $requisicoes . " requisições no último minuto...</>";
            $mensagem .= "\n <fg=gray>Proteção contra o Rate Limit do servidor. (20req/min)</>";
            $mensagem = $cvdw->getMensagem($mensagem);
            $progressBar->setMessage($mensagem);
            $progressBar->display();
            sleep(2);
        }

        $segundos = 60;
        $delay = 3;
        $esperar = 0;
        if ($diferenca < $segundos && $requisicoes > 19) {
            $esperar = $segundos - $diferenca + $delay;
        }

        return $esperar;
    }

    /**
     * @deprecated Countdown agora é feito dentro de requestCVDW
     */
    public function aguardar($cvdw, $progressBar, int $segundos = 3): void
    {
        $mensagem = null;
        for ($i = $segundos; $i > 0; $i--) {
            if ($i == 1) {
                $mensagem = ' <fg=blue>Aguardando ' . $i . ' segundo para a próxima requisição...</>';
            } else {
                $mensagem = ' <fg=blue>Aguardando ' . $i . ' segundos para a próxima requisição...</>';
            }
            $mensagem .= "\n <fg=gray>Proteção contra o Rate Limit do servidor. (20req/min)</>";
            $mensagem = $cvdw->getMensagem($mensagem);
            $progressBar->setMessage($mensagem);
            $progressBar->display();
            sleep(1);
        }
        $progressBar->setMessage($cvdw->getMensagem($mensagem));
    }

    /**
     * @deprecated Countdown agora é feito dentro de requestCVDW
     */
    protected function aguardarSemProgresso(int $segundos): void
    {
        $this->console->text("");
        $this->console->text("<fg=gray>Proteção contra o Rate Limit do servidor. (20req/min)</>");
        for ($i = $segundos; $i > 0; $i--) {
            if ($i == 1) {
                $this->console->text('<fg=blue>Aguardando ' . $i . ' segundo para a próxima requisição...</>');
            } else {
                $this->console->text('<fg=blue>Aguardando ' . $i . ' segundos para a próxima requisição...</>');
            }
            sleep(1);
        }
        $this->console->text(['', '']);
    }

    public function pingAmbienteCVDW(string $enderecoCv): array
    {
        $cabecalho = [
            $this::HEADER_CONTENT_TYPE,
        ];
        $url = $this::PROTOCOLO_HTTP . $enderecoCv . '.cvcrm.com.br/api/app/ambiente';
        $curl = curl_init();
        curl_setopt_array(
            $curl,
            [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_ENCODING => '',
                CURLOPT_MAXREDIRS => 10,
                CURLOPT_CONNECTTIMEOUT => 10,
                CURLOPT_TIMEOUT => 60,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                CURLOPT_CUSTOMREQUEST => 'GET',
                CURLOPT_HTTPHEADER => $cabecalho,
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSL_VERIFYHOST => false,
            ]
        );
        $response = curl_exec($curl);
        curl_close($curl);

        $response = json_decode($response, true);

        if (isset($response['Response']) && $response['Response'] == $this::RESPONSE_TOO_MANY_REQUESTS) {
            $this->console->error([
                $this::ERRO_REQUISICAO,
                $this::ERRO_BLOQUEIO,
                $this::TENTAR_NOVAMENTE,
                $response,
            ]);

            return [];
        }

        if (isset($response['nome'])) {
            return $response;
        } else {
            $this->console->error([
                $this::ERRO_REQUISICAO,
                $response,
            ]);

            return [];
        }
    }

    public function pingAmbienteAutenticadoCVDW(string $ambienteCv, string $path, string $email, string $token)
    {
        $cabecalho = [
            'email: ' . $email . '',
            'token: ' . $token . '',
            $this::HEADER_CONTENT_TYPE,
        ];
        $parametros = [
            "pagina" => "1",
        ];
        $url = $this::PROTOCOLO_HTTP . $ambienteCv . '.cvcrm.com.br/api/v1/cvdw' . $path;

        $curl = curl_init();
        curl_setopt_array(
            $curl,
            [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_ENCODING => '',
                CURLOPT_MAXREDIRS => 10,
                CURLOPT_CONNECTTIMEOUT => 10,
                CURLOPT_TIMEOUT => 60,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                CURLOPT_CUSTOMREQUEST => 'GET',
                CURLOPT_POSTFIELDS => json_encode($parametros),
                CURLOPT_HTTPHEADER => $cabecalho,
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSL_VERIFYHOST => false,
            ]
        );

        $response = curl_exec($curl);
        curl_close($curl);

        $responseJson = json_decode($response, true);

        if (isset($responseJson->Response) && $responseJson->Response == $this::RESPONSE_TOO_MANY_REQUESTS) {
            $this->console->error([
                $this::ERRO_REQUISICAO,
                $this::ERRO_BLOQUEIO,
                $this,
                $response,
            ]);

            return false;
        }

        if (isset($responseJson['registros']) && $responseJson['registros'] !== null) {
            return $responseJson;
        } else {
            $this->console->error([
                'Erro ao tentar fazer a requisição.',
                $response,
            ]);

            return false;
        }
    }

    public function buscarVersaoRepositorio()
    {
        $repo = 'manzano/cvdw-cli';
        $url = $this::PROTOCOLO_HTTP . "api.github.com/repos/$repo/releases/latest";
        $curl = curl_init();
        $cabecalho = ['User-Agent: Github / CVDW-CLI', 'Accept: application/json'];
        $verbose = fopen('php://temp', 'w+');
        curl_setopt_array(
            $curl,
            [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_ENCODING => '',
                CURLOPT_MAXREDIRS => 10,
                CURLOPT_CONNECTTIMEOUT => 10,
                CURLOPT_TIMEOUT => 20,
                CURLOPT_FOLLOWLOCATION => true,
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                CURLOPT_CUSTOMREQUEST => 'GET',
                CURLOPT_HTTPHEADER => $cabecalho,
                CURLOPT_SSL_VERIFYPEER => false,
                CURLOPT_SSL_VERIFYHOST => false,
                CURLOPT_VERBOSE => true,
                CURLOPT_STDERR => $verbose,
            ]
        );
        $response = curl_exec($curl);
        curl_close($curl);

        if ($response) {
            $data = json_decode($response, true);
            if (!isset($data['tag_name'])) {
                return "OFF";
            }

            return $data['tag_name'];
        } else {
            return "OFF";
        }
    }
}
