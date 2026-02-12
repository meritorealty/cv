<?php

namespace Manzano\CvdwCli\Services;

require_once __DIR__ . '/../Inc/Conexao.php';

use Manzano\CvdwCli\Services\Console\CvdwSymfonyStyle;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class RateLimit
{
    protected CvdwSymfonyStyle $console;
    public InputInterface $input;
    public OutputInterface $output;
    public \Doctrine\DBAL\Connection $conn;
    public DatabaseSetup $database;
    public object $resposta;
    public $logObjeto = false;
    public array $objeto;
    public $executarObj;
    public $idrequisicao;

    public $inicioExecucao;
    public $tempoLimiteExecucao;

    // ============================================================
    // NOVO: Sliding window em memória (não depende do DB/clock)
    // ============================================================
    private array $timestamps = [];
    private int $maxRequests = 18;       // Margem de segurança (limite real: 20)
    private int $windowSeconds = 60;
    private float $intervaloMinimo = 3.5; // Segundos entre requisições
    private float $ultimaRequisicaoTs = 0;

    // ============================================================
    // NOVO: Circuit Breaker
    // ============================================================
    private int $falhasConsecutivas = 0;
    private int $limiteAbrir = 3;        // Abre após 3 falhas 429 seguidas
    private ?float $abertoEm = null;
    private int $tempoRecuperacao = 120; // 2 minutos em estado aberto
    private bool $circuitoAberto = false;

    public function __construct(InputInterface $input, OutputInterface $output, $executarObj)
    {
        $this->input = $input;
        $this->output = $output;
        $this->executarObj = $executarObj;
        $this->conn = \Manzano\CvdwCli\Inc\Conexao::conectarDB($this->input, $this->output);
    }

    // ============================================================
    // NOVO: Aguardar sliding window + intervalo mínimo
    // ============================================================
    public function aguardarSeNecessario(): void
    {
        // 1) Intervalo mínimo entre requisições (3.5s)
        if ($this->ultimaRequisicaoTs > 0) {
            $decorrido = microtime(true) - $this->ultimaRequisicaoTs;
            if ($decorrido < $this->intervaloMinimo) {
                $espera = $this->intervaloMinimo - $decorrido;
                usleep((int) ($espera * 1_000_000));
            }
        }

        // 2) Sliding window: limpar timestamps fora da janela
        $agora = microtime(true);
        $this->timestamps = array_values(array_filter(
            $this->timestamps,
            fn($ts) => ($agora - $ts) < $this->windowSeconds
        ));

        // 3) Se atingiu o limite, esperar até liberar
        if (count($this->timestamps) >= $this->maxRequests) {
            $maisAntigo = min($this->timestamps);
            $espera = (int) ceil($this->windowSeconds - ($agora - $maisAntigo)) + 2;
            if ($espera > 0) {
                sleep($espera);
            }
            // Limpar novamente após espera
            $agora = microtime(true);
            $this->timestamps = array_values(array_filter(
                $this->timestamps,
                fn($ts) => ($agora - $ts) < $this->windowSeconds
            ));
        }
    }

    public function registrarRequisicaoMemoria(): void
    {
        $this->timestamps[] = microtime(true);
        $this->ultimaRequisicaoTs = microtime(true);
    }

    // ============================================================
    // NOVO: Circuit Breaker - métodos
    // ============================================================
    public function circuitoEstaAberto(): bool
    {
        if (!$this->circuitoAberto) {
            return false;
        }

        // Verificar se já passou o tempo de recuperação
        if ($this->abertoEm !== null) {
            $decorrido = microtime(true) - $this->abertoEm;
            if ($decorrido >= $this->tempoRecuperacao) {
                // Tentar fechar (half-open)
                $this->circuitoAberto = false;
                $this->falhasConsecutivas = 0;
                $this->abertoEm = null;
                return false;
            }
        }

        return true;
    }

    /**
     * Registra uma falha 429.
     * Retorna true se pode tentar novamente, false se circuito abriu.
     */
    public function registrarFalha429(): bool
    {
        $this->falhasConsecutivas++;
        if ($this->falhasConsecutivas >= $this->limiteAbrir) {
            $this->circuitoAberto = true;
            $this->abertoEm = microtime(true);
            return false; // Sinaliza: pular para próximo endpoint
        }
        return true; // Pode tentar novamente
    }

    public function registrarSucesso(): void
    {
        $this->falhasConsecutivas = 0;
    }

    public function getCircuitBreakerStatus(): string
    {
        if ($this->circuitoAberto) {
            $restante = $this->tempoRecuperacao - (microtime(true) - $this->abertoEm);
            return "ABERTO (recuperação em " . max(0, (int)$restante) . "s)";
        }
        return "FECHADO (falhas: {$this->falhasConsecutivas}/{$this->limiteAbrir})";
    }

    // ============================================================
    // Métodos existentes (mantidos para compatibilidade com DB log)
    // ============================================================
    public function inserirRequisicao($objeto): int
    {
        $queryBuilder = $this->conn->createQueryBuilder();
        $queryBuilder
            ->insert('_requisicoes')
            ->values([
                'data_inicio' => 'NOW()',
                'objeto' => ':objeto',
            ])
            ->setParameter('objeto', $objeto);
        $queryBuilder->executeStatement();
        $this->idrequisicao = $this->conn->lastInsertId();

        return $this->idrequisicao;
    }

    public function concluirRequisicao($idrequisicao, $dadosRetornoQtd = null, $headerResultado = null): void
    {
        $queryBuilder = $this->conn->createQueryBuilder();
        $queryBuilder
            ->update('_requisicoes')
            ->set('data_fim', 'NOW()')
            ->set('dados_retorno_qtd', ':dados_retorno_qtd')
            ->set('header_resultado', ':header_resultado')
            ->where('idrequisicao = :idrequisicao')
            ->setParameter('dados_retorno_qtd', $dadosRetornoQtd)
            ->setParameter('header_resultado', $headerResultado)
            ->setParameter('idrequisicao', $idrequisicao);
        $queryBuilder->executeStatement();
    }

    public function getDiferencaSegundosUltimaRequisicao(): ?int
    {
        $query = "SELECT TIMESTAMPDIFF(SECOND, data_inicio, NOW()) AS diferenca_segundos
                  FROM _requisicoes 
                  ORDER BY data_inicio DESC 
                  LIMIT 19,1";
        $stmt = $this->conn->executeQuery($query);
        $result = $stmt->fetchOne();
        return $result !== false ? (int) $result : null;
    }

    public function qtdRequisicoes($segundos = 60): ?int
    {
        $query = "SELECT COUNT(*) AS total_requisicoes
        FROM _requisicoes
        WHERE data_inicio >= NOW() - INTERVAL $segundos SECOND";
        $stmt = $this->conn->executeQuery($query);
        $result = $stmt->fetchOne();
        return (int) $result;
    }

    public function removerRequisicoesAntigas($dias = 30): ?int
    {
        $query = "DELETE FROM _requisicoes 
                  WHERE data_inicio < NOW() - INTERVAL $dias DAY";
        $stmt = $this->conn->executeQuery($query);
        $result = $stmt->rowCount();
        return $result;
    }

    public function iniciarExecucao(): float
    {
        $this->inicioExecucao = time();
        return $this->inicioExecucao;
    }

    public function tempoDeExecucao(): float
    {
        $tempoAtual = time();
        return $tempoAtual - $this->inicioExecucao;
    }

    public function validarTempoExecucao()
    {
        if ($this->tempoLimiteExecucao) {
            $tempoExecucao = $this->tempoDeExecucao();
            if ($tempoExecucao >= $this->tempoLimiteExecucao) {
                $this->console = new CvdwSymfonyStyle($this->input, $this->output, $this->logObjeto);
                $this->console->error("Tempo de execução excedido! Limite: {$this->tempoLimiteExecucao} segundos.");
                exit;
            }
        }
    }

    public function setTempoLimiteExecucao($tempoLimiteExecucao)
    {
        $this->tempoLimiteExecucao = $tempoLimiteExecucao;
    }
}
