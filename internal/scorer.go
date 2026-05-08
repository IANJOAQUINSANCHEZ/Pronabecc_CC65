package internal

import (
	"container/heap"
	"strings"
)

// PESOS DEL SISTEMA DE SCORING CONTENT-BASED
const (
	PesoNivel        = 3 // NIVEL ACADEMICO
	PesoGestion      = 2 // TIPO DE GESTIÓN
	PesoConvocatoria = 2 // NOMBRE DE BECA
	PesoSede         = 1 // SEDE/DISTRITO
)

// TOP-N RECOMENDACIONES
const TopN = 5

// SCORED BECA (BECA PUNTUADA)
type scoredBeca struct {
	beca  *Beca
	score int
}

// MIN HEAP PARA SELECCIONAR TOP-N EFICIENTE
type minHeap []scoredBeca

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].score < h[j].score }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(scoredBeca)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// CALCULA LA AFINIDAD SIN CONVERSIONES DE STRING
func calcularScoreRapido(est *Estudiante, beca *Beca) int {
	score := 0

	// NIVEL
	if est.TipoEstudiante != "" && beca.Nivel != "" {
		if strings.Contains(beca.Nivel, est.TipoEstudiante) || strings.Contains(est.TipoEstudiante, beca.Nivel) {
			score += PesoNivel
		}
	}

	// GESTION (Match real: verifica si la gestión de la beca coincide con el perfil del estudiante)
	if beca.TipoGestion != "" && strings.Contains(est.TipoEstudiante, beca.TipoGestion) {
		score += PesoGestion
	}

	// CONVOCATORIA
	if est.Convocatoria != "" {
		if strings.Contains(beca.Nombre, est.Convocatoria) {
			score += PesoConvocatoria
		}
	}

	// SEDE
	if est.Distrito != "" && beca.Sede != "" {
		if strings.Contains(beca.Sede, est.Distrito) || strings.Contains(est.Distrito, beca.Sede) {
			score += PesoSede
		}
	}

	return score
}

// EJECUTA PODA POR CLAVE COMPUESTA + SCORING + TOP-5.
func RecomendarConIndice(est *Estudiante, indice map[string][]*Beca, todasBecas []*Beca) []Recomendacion {
	h := &minHeap{}
	heap.Init(h)

	// PODA AGRESIVA POR CLAVE COMPUESTA
	becasCandidatas := indice[est.TipoEstudiante]

	// SI NO HAY MATCH POR ESTUDIANTE, BUSCAR PARCIAL
	if len(becasCandidatas) == 0 {
		for nivel, grupo := range indice {
			if strings.Contains(nivel, est.TipoEstudiante) || strings.Contains(est.TipoEstudiante, nivel) {
				becasCandidatas = append(becasCandidatas, grupo...)
			}
		}
	}

	// FALLBACK: SI SIGUE VACÍO, TOMAR UNA MUESTRA DEL CATÁLOGO
	if len(becasCandidatas) == 0 {
		limite := len(todasBecas)
		if limite > 50000 {
			limite = 50000
		}
		becasCandidatas = todasBecas[:limite]
	}

	// SCORING SOBRE CANDIDATAS
	for _, beca := range becasCandidatas {
		score := calcularScoreRapido(est, beca)
		if score == 0 {
			continue
		}

		if h.Len() < TopN {
			heap.Push(h, scoredBeca{beca: beca, score: score})
		} else if score > (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, scoredBeca{beca: beca, score: score})
		}
	}

	// EXTRAER RESULTADOS DEL HEAP
	resultados := make([]Recomendacion, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		item := heap.Pop(h).(scoredBeca)
		resultados[i] = Recomendacion{
			IDPostulante: est.IDPostulante,
			Rank:         i + 1,
			BecaNombre:   item.beca.Nombre,
			Score:        item.score,
			Nivel:        item.beca.Nivel,
			Institucion:  item.beca.Institucion,
			Sede:         item.beca.Sede,
		}
	}

	return resultados
}
