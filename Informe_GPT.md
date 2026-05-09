# Informe Técnico de Análisis de GAPs - Motor Concurrente PRONABEC

## Resumen Ejecutivo

El sistema analizado implementa un motor de recomendaciones concurrente en Go basado en `Worker Pool` con patrón `Fan-Out/Fan-In`, carga masiva de CSV e indexación para poda de candidatos. La arquitectura demuestra intención correcta en paralelismo y reducción de espacio de búsqueda, pero presenta brechas importantes de confiabilidad, seguridad operativa, mantenibilidad y fidelidad entre el modelo formal y la implementación real.

Los GAPs de mayor impacto son:

1. El procesamiento de CSV descarta errores y filas inválidas de forma silenciosa, lo que debilita la integridad de datos y la observabilidad operativa.
2. La organización del repositorio no es idiomática para Go y actualmente rompe `go test ./...` por múltiples `main` en los mismos paquetes, lo que afecta escalabilidad del desarrollo y CI/CD.
3. El `Worker Pool` no tiene cancelación, `context.Context`, timeouts internos ni control de backpressure más allá del tamaño fijo de los canales, por lo que un fallo del writer puede degradar o bloquear el pipeline.
4. La medición de memoria es técnicamente incorrecta: se reporta `TotalAlloc` como “Pico de RAM”, aunque ese campo representa memoria acumulada histórica y no memoria residente o pico real.
5. El modelo Promela/SPIN verifica una versión simplificada del protocolo de terminación que no coincide con la semántica real en Go basada en cierre de canales y `WaitGroup`, por lo que la ausencia de deadlocks en el modelo no prueba completamente la ausencia de fallos en producción.

En conjunto, el sistema es funcional como prototipo académico y experimento de concurrencia, pero todavía no alcanza un nivel robusto de ingeniería SRE para cargas reales de Big Data sostenidas.

## Estructura del Sistema Analizado

### Componentes principales

- Entrada y orquestación concurrente: [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:13>)
- Versión secuencial de referencia: [cmd/main_sec.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_sec.go:11>)
- Carga e indexación de datos: [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:13>)
- Lógica de scoring y poda: [internal/scorer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/scorer.go:41>)
- Escritura de resultados: [internal/writer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/writer.go:10>)
- Benchmark: [benchmark/runner.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/benchmark/runner.go:36>)
- Modelo formal: [verificacion/motor_concurrente.pml](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/verificacion/motor_concurrente.pml:1>)

### Flujo observado

1. Se cargan becas y estudiantes completos en memoria desde CSV.
2. Se construye un índice compuesto `Nivel|TipoGestion` con fallback por `Nivel`.
3. El productor envía estudiantes por `jobs`.
4. N workers calculan top-N recomendaciones.
5. Un único writer consume `results` y persiste en CSV.

### Hallazgos estructurales iniciales

- La ruta de entrada principal está hardcodeada a `Becas_1M_Definitivo.csv` en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:28>), pero ese archivo no está presente en el workspace actual. Esto es una brecha operativa de reproducibilidad.
- `go test ./...` falla en el estado actual porque existen múltiples programas `main` en el mismo paquete/directorio (`cmd` y raíz del módulo), lo que evidencia problemas de empaquetado y ausencia de pipeline de validación continua.

## Detalle de GAPs Identificados

### 1. Calidad de Código y Arquitectura

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| Estructura de paquetes no idiomática para Go; múltiples `main` en un mismo paquete/directorio | `cmd/main_con.go` y `cmd/main_sec.go` comparten el paquete `main` dentro del mismo directorio; además la raíz contiene varios `main` y símbolos duplicados, lo que rompe `go test ./...` | Imposibilita pruebas modulares, complica CI/CD, aumenta acoplamiento y reduce mantenibilidad | Alta |
| Orquestación de negocio concentrada en `main` | El flujo de carga, indexación, worker pool, writer y métricas está acoplado directamente en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:25>) | Dificulta pruebas unitarias, reuso y extensión del motor como librería o servicio | Alta |
| API `internal` poco cohesionada y de bajo nivel | `cmd` invoca directamente `CargarBecas`, `CargarEstudiantes`, `IndexarBecasCompuesto`, `RecomendarConIndice` y `EscribirResultadosStream` | La aplicación debe conocer demasiados detalles internos del pipeline | Media |
| Comentarios en mayúsculas y estilo no idiomático | Presente en varios archivos de `cmd` e `internal` | Afecta legibilidad y se aparta de `Effective Go`; no es falla funcional, pero sí de calidad | Baja |
| Hardcoding de parámetros operativos | Archivos de entrada, buffers de canal y `TopN` están codificados en fuente | Reduce configurabilidad y dificulta tuning por entorno | Media |
| Ausencia de pruebas automatizadas | `internal` y `benchmark` no tienen tests; `go test ./...` falla por estructura del módulo | No hay red de seguridad ante regresiones de scoring, indexado ni concurrencia | Alta |

### 2. Manejo de errores y observabilidad

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| Errores de lectura CSV descartados silenciosamente | `if err != nil { continue }` en [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:34>) y [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:83>) | Puede perder registros sin alertar; la salida final puede parecer válida con datos incompletos | Alta |
| Errores de parseo numérico ignorados | `ingresos, _ = strconv.ParseFloat(...)` en [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:97>) | Introduce default `0.0` silencioso y sesga recomendaciones o métricas | Alta |
| Errores de escritura CSV ignorados | `writer.Write(...)` y `file.WriteString(...)` no verifican error en [internal/writer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/writer.go:18>), [internal/writer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/writer.go:23>) y [internal/writer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/writer.go:65>) | Riesgo de truncamiento o corrupción no detectada en el archivo de salida | Alta |
| El writer puede fallar sin cancelar el pipeline | Si `EscribirResultadosStream` retorna error, solo se imprime en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:70>) | Los workers pueden seguir enviando resultados aunque el sink ya no sea confiable | Alta |
| Observabilidad limitada a `fmt.Printf` | No hay logs estructurados, métricas exportables, tracing ni contadores de filas descartadas | Dificulta diagnóstico en producción, SLOs y análisis post-mortem | Alta |
| Métrica de memoria mal interpretada | Se imprime `memStats.TotalAlloc` como “Pico de RAM” en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:125>) y [cmd/main_sec.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_sec.go:91>) | Induce decisiones incorrectas de capacidad porque `TotalAlloc` no es memoria actual ni máximo residente | Alta |

### 3. Seguridad

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| Riesgo de CSV Injection en exportación | Los campos `BecaNombre`, `Institucion`, `Sede` y otros se escriben tal cual en [internal/writer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/writer.go:65>) | Si un valor comienza con `=`, `+`, `-` o `@`, Excel/LibreOffice podría interpretarlo como fórmula | Alta |
| Falta de validación de esquema y dominios de datos | `FieldsPerRecord = -1` y `LazyQuotes = true` en [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:20>) y [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:69>) | Se aceptan entradas malformadas o ambiguas, elevando riesgo de datos inconsistentes o explotables | Alta |
| Sin límites de tamaño de archivo ni presupuesto de memoria | Los CSV completos se cargan en slices preasignados de gran tamaño en [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:28>) y [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:77>) | Riesgo de `resource exhaustion` por archivos mayores a los previstos o input hostil | Alta |
| Sin sanitización explícita de strings externos | La normalización actual es principalmente `TrimSpace` y `ToUpper` | No protege contra payloads de control, delimitadores inesperados o fórmulas embebidas | Media |
| Dependencia de rutas locales hardcodeadas | Lectura/escritura sobre nombres fijos de archivo en `cmd` | Riesgo operativo, colisión de archivos y baja portabilidad; no es una vulnerabilidad directa, pero sí una debilidad de despliegue | Media |

### 4. Patrones de Concurrencia y Escalabilidad

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| Sin `context.Context` ni cancelación cooperativa | El worker pool de [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:75>) depende solo de cierre de canales | Ante fallos parciales, no existe vía estándar para detener trabajo en vuelo ni drenar ordenadamente | Alta |
| Canales con buffer fijo y sin adaptación a carga | `jobs` y `results` usan tamaño `1000` fijo en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:54>) y [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:57>) | Puede haber contención por CPU, bursts hacia el writer o subutilización dependiendo del dataset y hardware | Media |
| Único writer serializa toda la salida | `EscribirResultadosStream` centraliza persistencia en una sola goroutine | El throughput global queda limitado por I/O de una sola etapa final | Media |
| La poda “O(1)” no se sostiene en todos los caminos | El índice se crea por `Nivel|TipoGestion`, pero la búsqueda usa `indice[est.TipoEstudiante]` en [internal/scorer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/scorer.go:79>) y luego recorre `for nivel, grupo := range indice` en [internal/scorer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/scorer.go:83>) | En misses la complejidad real deriva a exploración parcial del mapa y append de grupos, no a O(1) estricto | Alta |
| Scoring introduce sesgo lógico por gestión | `if beca.TipoGestion != "" { score += PesoGestion }` en [internal/scorer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/scorer.go:52>) | Todas las becas con gestión no vacía reciben puntaje adicional, sin compararse con el perfil del estudiante | Alta |
| Fallback de 50k becas puede disparar CPU y memoria | Slice `todasBecas[:limite]` en [internal/scorer.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/scorer.go:91>) | Bajo baja selectividad, muchos workers pueden procesar hasta 50k candidatos por estudiante | Alta |
| Uso extensivo de punteros para objetos inmutables sin beneficio claro | `[]*Beca` y `[]*Estudiante` se comparten entre goroutines | Aumenta presión de GC y complejidad de aliasing; no hay data race visible hoy, pero sí fragilidad para cambios futuros | Media |
| Sin verificación explícita de fuga de goroutines o cierre anómalo | No hay tests de `goroutine leak`, ni métricas de goroutines activas, ni hooks de shutdown | Riesgo de leaks al evolucionar el código o agregar sinks externos | Media |

### 5. Consumo de RAM

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| Carga total en memoria de becas y estudiantes | Los loaders construyen slices completos antes de procesar | Escala mal si el volumen crece más allá del dataset actual o si se ejecuta en nodos con memoria ajustada | Alta |
| Índice duplicado por clave compuesta y por nivel | `IndexarBecasCompuesto` inserta cada beca al menos dos veces en el mapa en [internal/loader.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/internal/loader.go:123>) | Duplica referencias y aumenta presión sobre heap y GC | Media |
| Métricas `Alloc` vs `Sys` no instrumentadas correctamente | Solo se lee `runtime.MemStats` al final y se reporta un campo incorrecto | No se puede distinguir heap activo, memoria retenida por el runtime ni costo de GC | Alta |
| La versión secuencial acumula todos los resultados antes de escribir | `resultados := make([]internal.ResultadoEstudiante, 0, len(estudiantes))` en [cmd/main_sec.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_sec.go:45>) | Sesga comparaciones de memoria frente a la versión concurrente y agrava RAM en baseline | Media |

### 6. Aspectos de Verificación Formal

| GAP | Evidencia | Impacto | Prioridad |
|---|---|---|---|
| El protocolo de terminación modelado no coincide con Go | Promela usa mensajes `DONE` en `jobs` y `results` en [verificacion/motor_concurrente.pml](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/verificacion/motor_concurrente.pml:33>) y [verificacion/motor_concurrente.pml](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/verificacion/motor_concurrente.pml:87>); Go usa `close(jobs)`, `wgWorkers.Wait()` y `close(results)` en [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:95>) y [cmd/main_con.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/cmd/main_con.go:101>) | La prueba formal no cubre exactamente los mismos invariantes ni las mismas transiciones de estado | Alta |
| El modelo abstrae demasiado la lógica de negocio y el backpressure | No hay representación de errores de I/O, writer fallando, cancelación, tamaños variables ni latencias | Puede demostrar ausencia de deadlock en una versión idealizada, no en la implementación real | Alta |
| El modelo fija constantes pequeñas | `NUM_ESTUDIANTES 6`, `NUM_WORKERS 3`, `BUFFER_* 4` | Útil para model checking, pero insuficiente para inferir comportamiento de saturación o starvation bajo carga alta | Media |
| Propiedades LTL limitadas | Solo se verifican completitud y orden de terminación en [verificacion/motor_concurrente.pml](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/verificacion/motor_concurrente.pml:137>) | Faltan propiedades de seguridad sobre pérdida de resultados, progreso del writer, unicidad y consistencia | Media |

## Recomendaciones de Mitigación y Mejora

### Prioridad Alta

1. Reestructurar el proyecto con layout idiomático de Go.
   Separar cada ejecutable en su propio subdirectorio, por ejemplo `cmd/recomendador-concurrente/main.go`, `cmd/recomendador-secuencial/main.go`, `cmd/etl-pronabec/main.go`.

2. Introducir una capa de aplicación.
   Crear un paquete de orquestación, por ejemplo `internal/app` o `internal/engine`, que encapsule:
   - carga de datos
   - construcción del índice
   - ejecución del worker pool
   - persistencia
   - métricas

3. Cambiar el manejo de errores de “best effort silencioso” a “contabilizado y observable”.
   Registrar cuántas filas fueron descartadas por:
   - CSV inválido
   - columnas insuficientes
   - parseo numérico fallido
   - validación de dominio

4. Agregar `context.Context` al pipeline completo.
   Todo worker, loader y writer debería poder detenerse por:
   - error fatal de I/O
   - timeout
   - cancelación manual
   - agotamiento de recursos

5. Corregir instrumentación de memoria.
   Reportar al menos:
   - `Alloc`
   - `HeapAlloc`
   - `Sys`
   - `NumGC`
   - máximo observado muestreado durante la ejecución, no solo al final

6. Corregir el scoring y la poda.
   El índice debería usar exactamente las claves con las que luego se consulta. Si el criterio real es `TipoEstudiante + TipoGestion`, hay que modelarlo explícitamente. El puntaje por gestión no debe sumarse solo por no estar vacío.

7. Mitigar CSV Injection.
   Escapar o prefijar con comilla simple los campos que comiencen con `=`, `+`, `-`, `@`, tab o retorno de carro antes de exportarlos.

8. Alinear Promela con la implementación real.
   Modelar:
   - cierre de canales
   - sincronización equivalente a `WaitGroup`
   - fallos del writer
   - cancelación
   - backpressure por buffer lleno

### Prioridad Media

1. Sustituir carga total por streaming o procesamiento por lotes cuando sea posible.
   Para millones de registros, conviene desacoplar:
   - catálogo indexado persistente o memory-mapped
   - carga de estudiantes por chunks
   - escritura incremental confirmada

2. Afinar el worker pool con benchmark por saturación real.
   El conjunto fijo `[2,4,8,12,16]` en [benchmark/runner.go](</C:/Users/USER/Documents/2026-1/Programacion Concurrente y Distribuida/Pronabecc_CC65/Pronabecc_CC65/benchmark/runner.go:23>) es útil como base, pero debe ampliarse con:
   - curvas de latencia por etapa
   - uso de CPU por core
   - presión de GC
   - throughput del writer

3. Reemplazar `fmt.Printf` por logging estructurado.
   Sugerencia:
   - `log/slog` en Go estándar
   - correlación por `run_id`
   - contadores por etapa

4. Añadir pruebas.
   Incluir:
   - tests unitarios del scoring e indexado
   - tests de integración del pipeline
   - tests con `-race`
   - pruebas de fuzzing sobre CSV

5. Reducir uso de punteros donde no sea necesario.
   Si los structs son pequeños y esencialmente inmutables, slices por valor pueden simplificar ownership y reducir aliasing accidental.

### Prioridad Baja

1. Normalizar comentarios y naming según `Effective Go`.
2. Parametrizar nombres de archivos y buffers desde flags o archivo de configuración.
3. Documentar invariantes del pipeline.
   Ejemplo:
   - quién cierra cada canal
   - bajo qué condición termina el writer
   - qué errores son recuperables y cuáles son fatales

## Conclusión

El motor concurrente PRONABEC tiene una base válida como prototipo de procesamiento concurrente en Go: usa correctamente un `Worker Pool`, desacopla cómputo de persistencia y muestra una intención explícita de verificar propiedades de concurrencia. Sin embargo, el análisis revela que las principales brechas no están en la idea general del patrón, sino en su endurecimiento para operación real: manejo silencioso de errores, seguridad de CSV, métricas de memoria equivocadas, empaquetado no idiomático y una verificación formal que hoy abstrae demasiado la semántica efectiva del código Go.

La prioridad técnica debe centrarse en volver el sistema verificable, observable y operable antes de buscar más optimización microarquitectónica. En términos SRE, el mayor riesgo actual no es un deadlock evidente, sino producir resultados incompletos o sesgados sin señal clara de fallo.

## Anexo de Validación

- Inspección estática de código fuente en `cmd`, `internal`, `benchmark` y `verificacion`.
- Verificación local de compilación:
  - `go build ./cmd/main_con.go`: exitoso
  - `go build ./cmd/main_sec.go`: exitoso
- Verificación del módulo completo:
  - `go test ./...`: falla por estructura de paquetes con múltiples `main` y símbolos redeclarados
- Limitación observada:
  - el archivo `Becas_1M_Definitivo.csv` referenciado por la versión concurrente no está presente en el workspace analizado, por lo que no se ejecutó una corrida end-to-end confiable sobre ese input.
