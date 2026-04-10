require('dotenv').config();
const axios = require('axios');
const AdmZip = require('adm-zip');
const { createClient } = require('@supabase/supabase-js');

// Conexões
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const gridApiGraphQL = axios.create({ baseURL: 'https://api.grid.gg/central-data/graphql', headers: { 'x-api-key': process.env.GRID_API_KEY, 'Content-Type': 'application/json' }});
const gridApiFiles = axios.create({ baseURL: 'https://api.grid.gg/file-download', headers: { 'x-api-key': process.env.GRID_API_KEY }});

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const toNum = (val) => {
  if (val === undefined || val === null || val === '') return 0;
  if (typeof val === 'number') return val;
  return parseFloat(val.toString().replace('%', '').replace(/\./g, '').replace(',', '.')) || 0;
};

// --- MOTOR DE RATINGS (Veio do seu React) ---
async function calculateRatings(players) {
  const { data: weights } = await supabase.from('lane_weights').select('*');
  const { data: bounds } = await supabase.from('lane_metrics_bounds').select('*');
  const localBounds = bounds || [];
  const updatedBounds = [];

  const playersWithDerived = players.map(p => {
    const opp = players.find(o => o.match_id === p.match_id && o.lane === p.lane && o.side !== p.side);
    const pi = p.cs_12 || 0, pj = opp?.cs_12 || pi;
    const ri = p.xp_12 || 0, rj = opp?.xp_12 || ri;
    const si = p.gold_12 || 0, sj = opp?.gold_12 || si;

    return {
      ...p, cs_diff_at_12: pi - pj, gold_diff_at_12: si - sj, xp_diff_at_12: ri - rj,
      _ap: ((pi / (pi + pj || 1)) + (ri / (ri + rj || 1)) + (si / (si + sj || 1))) / 3, 
      _aq: (p.dmg_percent || 0) / (p.gold_share || 1),
    };
  });

  const metricsMap = {
    ap: '_ap', p: 'cs_12', ar: 'cs_diff_at_12', as: 'gold_diff_at_12', at: 'xp_diff_at_12', l: 'deaths_at_12', ac: 'plates', al: 'vpm_at_12', am: 'kda_at_12',
    i: 'kda', k: 'kp', ae: 'taken_percent', ad: 'dmg_percent', aa: 'dmg_buildings', ab: 'dmg_objectives', v: 'dpm', an: 'cc_score',
    w: 'gpm', z: 'fpm', x: 'gold_efficiency', ao: 'gold_share', aq: '_aq', t: 'vspm', aj: 'wards_killed', ai: 'wards_placed', u: 'cw_placed'
  };

  const lanes = ['TOP', 'JNG', 'MID', 'ADC', 'SUP'];
  lanes.forEach(lane => {
    const lanePlayers = playersWithDerived.filter(p => p.lane?.toUpperCase() === lane);
    Object.keys(metricsMap).forEach(mKey => {
      const field = metricsMap[mKey];
      const vals = lanePlayers.map(p => p[field]);
      if (vals.length === 0) return;
      const cMin = Math.min(...vals), cMax = Math.max(...vals);

      let b = localBounds.find(b => b.lane === lane && b.metric_name === mKey);
      if (!b) {
        const newBound = { lane, metric_name: mKey, min_val: cMin, max_val: cMax };
        updatedBounds.push(newBound);
        localBounds.push(newBound);
      } else if (cMin < b.min_val || cMax > b.max_val) {
        b.min_val = Math.min(b.min_val, cMin); b.max_val = Math.max(b.max_val, cMax);
        updatedBounds.push(b);
      }
    });
  });

  if (updatedBounds.length > 0) await supabase.from('lane_metrics_bounds').upsert(updatedBounds);

  return playersWithDerived.map(p => {
    const lane = p.lane?.toUpperCase();
    const lW = weights?.find(w => w.lane === lane);

    const calc = (keys) => {
      let sumW = 0, sumN = 0;
      keys.forEach(k => {
        const weight = toNum(lW?.[`w_${k}`]);
        if (weight === 0) return;
        const b = localBounds.find(b => b.lane === lane && b.metric_name === k);
        const val = p[metricsMap[k]];
        let norm = (!b || b.max_val === b.min_val) ? 0.5 : (val - b.min_val) / (b.max_val - b.min_val);
        if (k === 'l') norm = 1 - norm; 
        sumN += norm * weight; sumW += weight;
      });
      return Math.max(50, 50 + (50 * (sumN / (sumW || 1))));
    };

    const { _ap, _aq, ...cleanPlayer } = p;
    return {
      ...cleanPlayer, lane_efficiency: _ap, dmg_gold_ratio: _aq,
      lane_rating: calc(['ap', 'p', 'ar', 'as', 'at', 'l', 'ac', 'al', 'am']),
      impact_rating: calc(['i', 'k', 'ae', 'ad', 'aa', 'ab', 'v', 'an']),
      conversion_rating: calc(['w', 'z', 'x', 'ao', 'aq']), vision_rating: calc(['t', 'aj', 'ai', 'u'])
    };
  });
}

// --- FUNÇÕES DE EXTRAÇÃO DE DADOS EM MEMÓRIA ---
// Obs: As funções extrairEstatisticasDB, extrairDadosPorTempo, etc. ficam aqui, 
// idênticas às que usamos no script anterior (o trator sem vazamento de RAM).
// Por brevidade visual aqui no chat, assuma que elas estão coladas aqui exatamente 
// como no código anterior, retornando os objetos { match_id, summoner_name, etc. }

async function baixarZIPSeguro(url) {
  try {
    const res = await gridApiFiles.get(url, { responseType: 'arraybuffer' });
    const buffer = Buffer.from(res.data);
    if (buffer[0] === 0x50 && buffer[1] === 0x4B) return new AdmZip(buffer); 
    return null;
  } catch (e) { return null; }
}

async function processarPartidaRecente(partida) {
    const seriesId = String(partida.id);
    console.log(`\n⏳ Baixando ZIPs da Série [${seriesId}] - ${partida.campeonato}`);

    const zipRiot = await baixarZIPSeguro(`/end-state/riot/series/${seriesId}`);
    if (!zipRiot) return; // Se não tem ZIP, a série não acabou ou não tem dados.
    await delay(1500); 
    
    // ... Aqui entra a lógica exata de ler os ZIPs em memória, extrair os times (uniqueTeamsMap), 
    // jogadores (uniquePlayersMap) e matches do seu React, dar o .upsert() neles, 
    // rodar os statsBrutos pelo calculateRatings(), e dar o insert final. 
    // (A mesma lógica estruturada no final do código anterior).
    console.log(`✅ Série ${seriesId} finalizada e salva no banco.`);
}

// --- O NOVO RADAR DIÁRIO ---
async function buscarEProcessarUltimas24h() {
  console.log(`\n🤖 LIGANDO RADAR DE EXTRAÇÃO DIÁRIA...`);
  
  // Calcula a data de ontem para hoje
  const ontem = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  
  const query = `
    query {
      allSeries(
        first: 50, 
        filter: { 
          titleId: 3, 
          startTimeScheduled: { gte: "${ontem}" }
        }, 
        orderBy: StartTimeScheduled, orderDirection: DESC
      ) {
        edges { node { id, tournament { name } } }
      }
    }`;

  try {
    const res = await gridApiGraphQL.post('', { query });
    const partidas = res.data.data.allSeries.edges.map(e => ({ id: e.node.id, campeonato: e.node.tournament?.name || 'Geral' }));
    
    console.log(`📡 Encontradas ${partidas.length} partidas agendadas/jogadas nas últimas 24h.`);
    
    for (const p of partidas) {
        // Checa se a série já está no banco para não reprocessar à toa
        const { data: existe } = await supabase.from('series').select('id').eq('id', p.id).maybeSingle();
        if (!existe) {
            await processarPartidaRecente(p);
        } else {
            console.log(`⏩ Série ${p.id} já existe no banco. Pulando.`);
        }
    }
    console.log("\n🏁 EXECUÇÃO DIÁRIA CONCLUÍDA.");
  } catch (err) {
    console.error(`❌ Erro no Radar:`, err.message);
  }
}

buscarEProcessarUltimas24h();