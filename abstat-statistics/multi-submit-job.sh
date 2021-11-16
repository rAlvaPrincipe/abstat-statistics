#!/bin/bash
function func(){
    dataset="./Datasets/linguistics/"$1".nt"
    output="./outputs/"$1
    PLD=${*: 2}
    ./abstat-statistics/submit-job.sh $dataset $output $PLD
}

#func apertium-rdf-ca-it http://linguistic.linkeddata.es
#func apertium-rdf-en-ca http://linguistic.linkeddata.es
#func apertium-rdf-en-es http://linguistic.linkeddata.es
#func apertium-rdf-en-gl http://linguistic.linkeddata.es
#func apertium-rdf-eo-ca http://linguistic.linkeddata.es
#func apertium-rdf-eo-en http://linguistic.linkeddata.es
#func apertium-rdf-eo-es http://linguistic.linkeddata.es
#func apertium-rdf-eo-fr http://linguistic.linkeddata.es
#func apertium-rdf-es-an http://linguistic.linkeddata.es
#func apertium-rdf-es-ast http://linguistic.linkeddata.es
#func apertium-rdf-es-gl http://linguistic.linkeddata.es
#func apertium-rdf-es-pt http://linguistic.linkeddata.es
#func apertium-rdf-es-ro http://linguistic.linkeddata.es
#func apertium-rdf-eu-en http://linguistic.linkeddata.es
#func apertium-rdf-eu-es http://linguistic.linkeddata.es
#func apertium-rdf-fr-ca http://linguistic.linkeddata.es
#func apertium-rdf-fr-es http://linguistic.linkeddata.es
#func apertium-rdf-oc-ca http://linguistic.linkeddata.es
#func apertium-rdf-oc-es http://linguistic.linkeddata.es
#func apertium-rdf-pt-ca http://linguistic.linkeddata.es
#func apertium-rdf-pt-gl http://linguistic.linkeddata.es
#func asit http://purl.org/asit/resource/
#func basque-eurowordnet-lemon-lexicon-3-0 http://lodserver.iula.upf.edu/id/WordNetLemon/ENGL/ http://lodserver.iula.upf.edu/id/WordNetLemon/EN/ http://lodserver.iula.upf.edu/id/WordNetLemon/GL/
#func catalan-eurowordnet-lemon-lexicon-3-0 http://lodserver.iula.upf.edu/id/WordNetLemon/CAT/ http://lodserver.iula.upf.edu/id/WordNetLemon/EN/ http://lodserver.iula.upf.edu/id/WordNetLemon/ENCAT/
#func cedict http://rdf.naturallexicon.org/zh/ont# http://rdf.naturallexicon.org/en/ont# http://rdf.naturallexicon.org/ont#
#func dbpedia-spotlight-nif-ner-corpus http://www.yovisto.com/labs/ner-benchmarks/data/f1e5ea4a1b76deb06c733588c221a31c
#func english-wordnet https://en-word.net/lemma/able https://en-word.net/lemma/ https://en-word.net/id/
#func galician-eurowordnet-lemon-lexicon-3-0 http://lodserver.iula.upf.edu/id/WordNetLemon/ENGL/ http://lodserver.iula.upf.edu/id/WordNetLemon/EN/ http://lodserver.iula.upf.edu/id/WordNetLemon/GL/
#func geodomainwn http://www.languagelibrary.eu/owl/geodomainWN/eng/instances/
#func germlex http://www.languagelibrary.eu/owl/geodomainWN/eng/instances/
#func getty-aat http://vocab.getty.edu/aat/
func gwa-ili http://globalwordnet.org/ili/
func iate http://tbx2rdf.lider-project.eu/data/iate/ http://tbx2rdf.lider-project.eu/data/iate/subjectField
func iso-639-oasis http://psi.oasis-open.org/iso/639/
func kore-50-nif-ner-corpus http://www.mpi-inf.mpg.de/yago-naga/aida/download/KORE50.tar.gz/AIDA.tsv/
func lemonuby http://lemon-model.net/lemon#
func lemonwiktionary http://lemon-model.net/lexica/wiktionary_en/en/
func lexvo http://lexvo.org/
func linked-hypernyms http://dbpedia.org/resource/
func news-100-nif-ner-corpus http://aksw.org/N3/News-100/
func reuters-128-nif-ner-corpus http://aksw.org/N3/Reuters-128/
func rss-500-nif-ner-corpus http://aksw.org/N3/RSS-500/
func saldo-rdf http://spraakbanken.gu.se/rdf/saldo#
func saldom-rdf http://spraakbanken.gu.se/rdf/saldo#
func sli_galnet_rdf http://sli.uvigo.gal/rdf_galnet_cat/
func srcmf http://www.srcmf.org/
func vu-wordnet http://wordnet-rdf.princeton.edu/rdf/
func w3c-wordnet http://www.w3.org/2006/03/wn/wn20/instances/
func wn-wiki-instances http://globalwordnet.org/ili/
func wordnet-rdf http://wordnet-rdf.princeton.edu/rdf/lemma/
#func xwn
func ysa http://www.yso.fi/onto/ysa/
