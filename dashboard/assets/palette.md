# FoodCost Monitor — Identidade Visual do Dashboard

Paleta e logo fictícios para o dashboard Power BI deste projeto. Estilo inspirado no
universo visual do iFood (vermelho vibrante, cartão/emblema arredondado) mas com
paleta, ícone e wordmark próprios — não é uma cópia da marca real.

## Conceito da logo

Emblema (rounded badge) em gradiente vermelho→laranja, com um ícone que combina:

- **Sacola** (nod ao universo de delivery) com uma faixa horizontal na cor da marca.
- **Linha de tendência ascendente** com um marcador (ponto dourado), representando
  o monitoramento de custo e a detecção de anomalias — o produto deste projeto,
  não um app de pedidos.

Arquivos entregues em `dashboard/assets/`:

| Arquivo | Uso |
|---|---|
| `logo-icon.svg` | Ícone isolado (240×240). Favicon, capa de página, espaços pequenos. |
| `logo-full.svg` | Lockup ícone + wordmark, para fundos claros. |
| `logo-full-dark.svg` | Lockup ícone + wordmark, para fundos escuros. |
| `theme.json` | Tema customizado do Power BI (cores). |
| `background-pattern.png` | Fundo de página (1920×1080, 16:9) com textura de pontos e ícones da marca em baixa opacidade. |
| `generate_background.py` | Script Python (Pillow) que gera o `background-pattern.png` — reexecute para variar semente/quantidade de ícones. |

Todos em **SVG vetorial** (sem perda de qualidade em qualquer tamanho). Não há
Inkscape/Cairo instalados nesta máquina para gerar PNG automaticamente — se
precisar de PNG (ex.: para o visual "Image" do Power BI, que não aceita SVG),
abra o SVG no navegador e use "Salvar como imagem"/print para PNG, ou uma
ferramenta online de conversão.

## Paleta de cores

### Marca

| Papel | Hex | Uso |
|---|---|---|
| Primária (brand) | `#DC2626` | Ícone, títulos de destaque, elementos de marca |
| Secundária (accent) | `#F97316` | Gradiente do emblema, destaques secundários |
| Texto principal | `#1F2937` | Texto em fundo claro |
| Texto secundário | `#6B7280` | Subtítulos, tagline "MONITOR" |

### Neutros (fundo dos relatórios)

| Papel | Hex |
|---|---|
| Fundo | `#FFFFFF` |
| Fundo alternativo | `#F9FAFB` |
| Fundo neutro (cards) | `#F3F4F6` |

### Semântica (status / anomalias — alinhado à lógica de `mart_anomalies`)

| Papel | Hex | Corresponde a |
|---|---|---|
| Bom / normal | `#16A34A` | Sem anomalia |
| Atenção | `#F59E0B` | Severidade `warning` (2–3σ) |
| Crítico | `#DC2626` | Severidade `critical` (>3σ) |

### Categórica (6 domínios: orders, payments, fintech, marketplace, logistics, restaurants)

Usada em `dataColors` do tema — mantém a ordem abaixo para consistência entre
gráficos (donut, barras, tendência):

1. `#DC2626` — orders
2. `#F97316` — payments
3. `#CA8A04` — fintech
4. `#0EA5E9` — marketplace
5. `#6366F1` — logistics
6. `#16A34A` — restaurants

## Fundo de página (`background-pattern.png`)

Gerado programaticamente (Pillow), não à mão em SVG — mais confiável para textura
repetida/gradiente do que path SVG longo. Composição, em camadas:

1. Base neutra quase branca (`#FCFBFA`), não branco puro.
2. Wash diagonal sutil na cor da marca (`#DC2626`, ~2% de opacidade máxima).
3. Grid de pontos finos (`#1F2937`, ~4% de opacidade) — textura "de dados".
4. ~22 ícones da marca (sacola, moeda, tendência, relógio, check) espalhados,
   girados e em baixa opacidade (7–13%), concentrados nas bordas/cantos —
   o centro fica mais "limpo" para não brigar com cards e gráficos.

Pensado para ser o **canvas background de todas as páginas** — por isso a
opacidade é baixa: ele deve aparecer como uma textura de fundo, não competir
com os dados.

## Como aplicar no Power BI Desktop

**Tema de cores:**
`Exibição/View` → `Temas/Themes` → `Procurar temas/Browse for themes` →
selecionar `dashboard/assets/theme.json`.

**Logo:**
`Inserir/Insert` → `Imagem/Image` → selecionar `logo-full.svg` (ou `logo-icon.svg`
para o ícone da página/cabeçalho). Use a variante `-dark` se o fundo da página
for escuro.

**Fundo de página:**
Clique na área vazia do canvas → painel `Formatar página/Format page` →
`Canvas background` → `Add image` → selecionar `background-pattern.png` →
`Image fit: Fit` (mantém as proporções 16:9, sem distorcer). Repita em cada
página, ou aplique e use "Sync visuals"/tema de página se o seu relatório
usar template de página.
