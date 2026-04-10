require('dotenv').config();
const axios = require('axios');
const AdmZip = require('adm-zip');
const { createClient } = require('@supabase/supabase-js');

// --- CONEXÕES ---
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const gridApiGraphQL = axios.create({ baseURL: 'https://api.grid.gg/central-data/graphql', headers: { 'x-api-key': process.env.GRID_API_KEY, 'Content-Type': 'application/json' }});
const gridApiFiles = axios.create({ baseURL: 'https://api.grid.gg/file-download', headers: { 'x-api-key': process.env.GRID_API_KEY }});

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- UTILIDADES ---
const toNum = (val) => {
  if (val === undefined || val === null || val === '') return 0;
  if (typeof val === 'number') return val;
  return parseFloat(val.toString().replace('%', '').replace(/\./g, '').replace(',', '.')) || 0;
};

function formatTournamentDetails(campeonato) {
  const campLow = campeonato.toLowerCase();
  if (campLow.includes("scrim")) return { game_type: "SCRIM", split: "N/A" };
  
  let game_type = campeonato;
  let split = "";
  if (campeonato.includes(" - ")) {
    const parts = campeonato.split(" - ");
    game_type = parts[0].trim(); 
    let rest = parts.slice(1).join(" - ").trim();
    if (rest.includes("(")) split = rest.split("(")[0].trim(); else split = rest;
  } else if (campeonato.includes("(")) {
    game_type = campeonato.split("(")[0].trim();
  }
  split = split.replace(/\b20\d{2}\b/g, '').replace(/\s{2,}/g, ' ').trim();
  return { game_type, split: split || "Geral" };
}

// --- FUNÇÕES DE EXTRAÇÃO DE DADOS EM MEMÓRIA ---
function extrairDadosPorTempo(endStateDetails, temposMinutos) {
  const temposMs = temposMinutos.map(min => min * 60000);
  const resultado = {};
  if (!endStateDetails || !endStateDetails.frames) return resultado;
  for (const tempoMs of temposMs) {
    const frame = endStateDetails.frames.find(f => f.timestamp >= tempoMs);
    if (!frame) continue;
    const dados = {};
    for (const [id, frameData] of Object.entries(frame.participantFrames)) {
      dados[id] = { cs: (frameData.minionsKilled || 0) + (frameData.jungleMinionsKilled || 0), xp: frameData.xp || 0, gold: frameData.totalGold || 0 };
    }
    resultado[tempoMs / 60000] = dados;
  }
  return resultado;
}

function extrairEventosChave(eventLines) {
  const resultado = {};
  for (const evento of eventLines) {
    const tipo = evento.eventSubType || evento.name || "";
    const ator = evento.actorName || evento.killerName || ""; 
    const assistentes = evento.assisters || [];
    if (evento.eventType === "CHAMPION_KILL" && tipo === "KILL_FIRST_BLOOD") {
      resultado[ator] = { ...resultado[ator], fbKill: true };
      assistentes.forEach(name => { resultado[name] = { ...resultado[name], fbAssist: true }; });
    }
    if (evento.eventType === "BUILDING_KILL" && tipo === "KILL_FIRST_TOWER") {
      resultado[ator] = { ...resultado[ator], ftKill: true };
      assistentes.forEach(name => { resultado[name] = { ...resultado[name], ftAssist: true }; });
    }
  }
  return resultado;
}

function extrairEstatisticasDB(jogo, detailsJson, eventLines, matchId, teamMap, patch) {
  const eventosChave = extrairEventosChave(eventLines);
  const dadosPorTempo = extrairDadosPorTempo(detailsJson, [6, 12, 18]); 
  const duracaoMinutos = (jogo.gameDuration || 1) / 60;
  const limit12 = 12 * 60 * 1000;
  const teamStats = { 100: { dmg: 0, gold: 0, taken: 0 }, 200: { dmg: 0, gold: 0, taken: 0 } };
  
  jogo.participants.forEach(p => {
    const tId = p.teamId || p.teamID;
    if (!teamStats[tId]) teamStats[tId] = { dmg: 0, gold: 0, taken: 0 };
    teamStats[tId].dmg += p.totalDamageDealtToChampions || (p.damageStats ? p.damageStats.totalDamageDoneToChampions : 0) || 0;
    teamStats[tId].gold += p.goldEarned || p.totalGold || p.currentGold || 0;
    teamStats[tId].taken += p.totalDamageTaken || (p.damageStats ? p.damageStats.totalDamageTaken : 0) || 0;
  });

  return jogo.participants.map(p => {
    const tId = p.teamId || p.teamID;
    const summoner = p.riotIdGameName || p.summonerName || (p.riotId ? p.riotId.displayName : "Unknown");
    const pId = p.participantId || p.participantID;

    const kills12 = eventLines.filter(e => e.eventType === "CHAMPION_KILL" && (e.killerName === summoner || e.killerId === pId) && (e.gameTime || e.timestamp || 0) <= limit12).length;
    const deaths12 = eventLines.filter(e => e.eventType === "CHAMPION_KILL" && (e.victimName === summoner || e.victimId === pId) && (e.gameTime || e.timestamp || 0) <= limit12).length;
    const assists12 = eventLines.filter(e => e.eventType === "CHAMPION_KILL" && (e.assisters || []).includes(summoner) && (e.gameTime || e.timestamp || 0) <= limit12).length;
    const wards12 = eventLines.filter(e => {
        const isWard = e.rfc461Schema === "ward_placed" || e.type === "WARD_PLACED" || e.rfc461Schema === "ward_kill" || e.type === "WARD_KILL";
        const isMine = e.placer?.toString() === pId.toString() || e.creatorId?.toString() === pId.toString() || e.killerId?.toString() === pId.toString();
        const time = e.gameTime || e.timestamp || 0;
        return isWard && isMine && time <= limit12;
    }).length;

    const dmg = p.totalDamageDealtToChampions || (p.damageStats ? p.damageStats.totalDamageDoneToChampions : 0) || 0;
    const taken = p.totalDamageTaken || (p.damageStats ? p.damageStats.totalDamageTaken : 0) || 0;
    const gold = p.goldEarned || p.totalGold || 0;
    const minions = (p.totalMinionsKilled || p.minionsKilled || 0) + (p.neutralMinionsKilled || p.jungleMinionsKilled || 0);
    
    let isWin = p.win;
    if (isWin === undefined && jogo.teams) {
        const teamObj = jogo.teams.find(t => (t.teamId || t.teamID) === tId);
        if (teamObj) isWin = teamObj.win;
    }

    let timestamp = jogo.gameStartTimestamp || jogo.gameCreation || Date.now();
    if (isNaN(new Date(timestamp).getTime())) timestamp = Date.now();

    return {
      match_id: matchId, summoner_name: summoner, puuid: p.puuid || `${summoner}-${matchId}`, 
      game_start_time: new Date(timestamp).toISOString(), patch: patch, 
      team_acronym: teamMap[tId] || (tId === 100 ? "BLUE" : "RED"), side: tId === 100 ? 'Blue' : 'Red',
      lane: p.lane || p.teamPosition || p.role || "UNKNOWN", champion: p.championName,
      kda: toNum(p.challenges?.kda || (p.deaths ? (p.kills + p.assists) / p.deaths : (p.kills + p.assists))),
      kills: p.kills || 0, deaths: p.deaths || 0, deaths_at_12: deaths12, assists: p.assists || 0,
      result: isWin ? "Victory" : "Defeat", cs_6: dadosPorTempo[6]?.[pId]?.cs || 0, cs_12: dadosPorTempo[12]?.[pId]?.cs || 0, cs_18: dadosPorTempo[18]?.[pId]?.cs || 0, 
      xp_12: dadosPorTempo[12]?.[pId]?.xp || 0, gold_12: dadosPorTempo[12]?.[pId]?.gold || 0,
      vpm_at_12: toNum(wards12 / 12), kda_at_12: deaths12 > 0 ? toNum((kills12 + assists12) / deaths12) : kills12 + assists12,
      vspm: toNum((p.visionScore || 0) / duracaoMinutos), cw_placed: p.detectorWardsPlaced || p.visionWardsBoughtInGame || 0,
      dpm: toNum(dmg / duracaoMinutos), gpm: toNum(gold / duracaoMinutos), gold_efficiency: toNum(gold / duracaoMinutos / 4.5),
      kp: toNum(p.challenges?.killParticipation), fpm: toNum(minions / duracaoMinutos),
      dmg_buildings: p.damageDealtToBuildings || 0, dmg_objectives: p.damageDealtToObjectives || 0, plates: p.challenges?.turretPlatesTaken || 0,
      dmg_percent: toNum(dmg / (teamStats[tId]?.dmg || 1)), taken_percent: toNum(taken / (teamStats[tId]?.taken || 1)), mitigated: p.totalDamageSelfMitigated || 0,
      fb_assist: eventosChave[summoner]?.fbAssist || false, fb_kill: eventosChave[summoner]?.fbKill || false,
      ft_assist: eventosChave[summoner]?.ftAssist || false, ft_kill: eventosChave[summoner]?.ftKill || false,
      total_dmg: dmg, total_taken: taken, vision_score: p.visionScore || 0, wards_placed: p.wardsPlaced || 0, wards_killed: p.wardsKilled || 0,
      total_gold: gold, win: isWin ? true : false, cc_score: p.timeCCingOthers || 0, gold_share: toNum(gold / (teamStats[tId]?.gold || 1))
    };
  });
}

function extrairWardsDB(eventLines, matchId, nameMap) {
  const wards = [];
  eventLines.forEach(e => {
    if (e.rfc461Schema === "ward_placed" || e.type === "WARD_PLACED") {
      const pid = e.placer?.toString() || e.creatorId?.toString();
      wards.push({
        match_id: matchId, player_name: nameMap[pid] || pid, minute: Math.floor((e.gameTime || e.timestamp || 0) / 60000),
        type: e.wardType || "unknown", ward_x: toNum(e.position?.x), ward_y: toNum(e.position?.z || e.position?.y)
      });
    }
  });
  return wards;
}

function extrairObjetivosDB(eventLines, matchId, teamMap, nameMap) {
  const objetivos = [];
  let dragonCount = 0;
  eventLines.forEach(e => {
    if (e.eventType === "ELITE_MONSTER_KILL" || e.rfc461Schema === "epic_monster_kill" || e.type === "ELITE_MONSTER_KILL") {
      const type = e.monsterType || e.epicMonsterType || "UNKNOWN";
      let objName = type;
      if (type === "DRAGON") { dragonCount++; objName = `dragon${dragonCount}`; }
      const killerId = e.killerId || e.killerID;
      let killerName = e.killerName;
      if (!killerName && killerId && nameMap[killerId]) killerName = nameMap[killerId];

      objetivos.push({
        match_id: matchId, minuto: Math.floor((e.timestamp || e.gameTime || 0) / 60000),
        team_acronym: teamMap[e.killerTeamId || e.teamId] || "N/A", objective_type: objName,
        subtype: e.monsterSubType || e.epicMonsterSubType || "", player_name: killerName || "Team"
      });
    }
  });
  return objetivos;
}

function extrairDraftsDB(gridState, endStateRiot, realMatchId, teamMap, gameNum) {
  const drafts = [];
  if (!gridState?.seriesState?.games) return drafts;

  const jogoNoGrid = gridState.seriesState.games[gameNum - 1];
  if (!jogoNoGrid?.draftActions) return drafts;

  const drafterSideMap = {};
  if (jogoNoGrid.teams) {
    jogoNoGrid.teams.forEach(t => {
      drafterSideMap[t.id] = t.side ? t.side.charAt(0).toUpperCase() + t.side.slice(1).toLowerCase() : 'Unknown';
    });
  }

  const champMap = {};
  if (endStateRiot?.participants) {
      endStateRiot.participants.forEach(p => {
        const side = (p.teamId || p.teamID) === 100 ? 'Blue' : 'Red';
        champMap[`${p.championName}_${side}`] = p.riotIdGameName || p.summonerName || (p.riotId ? p.riotId.displayName : "Unknown");
      });
  }

  const actions = jogoNoGrid.draftActions || [];
  actions.sort((a, b) => +(a.sequenceNumber || 0) - +(b.sequenceNumber || 0)).forEach(action => {
    const seq = parseInt(action.sequenceNumber, 10) || 0;
    const tipo = (action.type || "").toUpperCase();
    const campeao = action.draftable?.name || 'Unknown';
    const side = drafterSideMap[action.drafter?.id] || 'Unknown';
    
    drafts.push({
      match_id: realMatchId, team_acronym: side === 'Blue' ? teamMap[100] : teamMap[200],
      tipo: tipo, side: side, jogador: tipo === 'PICK' ? (champMap[`${campeao}_${side}`] || 'Team') : 'Team', champion: campeao, sequence: seq
    });
  });
  return drafts;
}

// --- MOTOR DE RATINGS ---
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

// --- ENGINE PRINCIPAL ---
async function baixarZIPSeguro(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await gridApiFiles.get(url, { responseType: 'arraybuffer' });
      const buffer = Buffer.from(res.data);
      if (buffer[0] === 0x50 && buffer[1] === 0x4B) return new AdmZip(buffer); 
      return null;
    } catch (e) {
      const status = e.response?.status;
      if (status === 429) {
        const waitTime = (i + 1) * 3000;
        console.log(`      ↳ ⏳ Rate Limit (429). Tentativa ${i+1}/${retries}. Esperando ${waitTime/1000}s...`);
        await delay(waitTime);
        continue;
      }
      if (status === 404) {
          console.log(`      ↳ ⚠️ 404: Arquivo inexistente na Grid.`);
          return null; 
      }
      console.log(`      ↳ ⚠️ Erro ${status || e.message} em: ${url}`);
      return null;
    }
  }
  return null;
}

async function processarPartidaRecente(partida) {
    const seriesId = String(partida.id);
    console.log(`\n⏳ Iniciando Série [${seriesId}] - ${partida.campeonato}`);

    console.log(`      ↳ Baixando: Riot End State...`);
    const zipRiot = await baixarZIPSeguro(`/end-state/riot/series/${seriesId}`);
    
    // Freio OBRIGATÓRIO de limite de taxa independente de erro ou sucesso
    await delay(1600); 

    if (!zipRiot) {
        console.log(`      ↳ ⏭️ Sem dados Riot (404/Erro). Pulando série...`);
        return;
    }

    console.log(`      ↳ Baixando: Riot Details...`);
    const zipDetails = await baixarZIPSeguro(`/end-state-details/riot/series/${seriesId}`);
    await delay(1600); 

    console.log(`      ↳ Baixando: Riot Events...`);
    const zipEvents = await baixarZIPSeguro(`/events/riot/series/${seriesId}`);
    await delay(1600); 

    console.log(`      ↳ Baixando: Grid End State...`);
    const zipGrid = await baixarZIPSeguro(`/end-state/grid/series/${seriesId}`);
    await delay(1600);

    const entradasRiot = zipRiot.getEntries();
    
    let gridState = null;
    if (zipGrid && zipGrid.getEntries) {
        const gridEntry = zipGrid.getEntries()[0];
        if (gridEntry) gridState = JSON.parse(gridEntry.getData().toString('utf8'));
    }

    const { game_type, split } = formatTournamentDetails(partida.campeonato);

    for (const entrada of entradasRiot) {
        const matchRegex = entrada.entryName.match(/_(\d+)\.json$/);
        if (!matchRegex) continue;
        
        const gameNum = matchRegex[1];
        const realMatchId = `${seriesId}_${gameNum}`;

        const { data: existe } = await supabase.from('matches').select('id').eq('id', realMatchId).maybeSingle();
        if (existe) {
             console.log(`⏩ Jogo ${realMatchId} já no BD. Pulando...`);
             continue;
        }

        console.log(`▶️ Processando Jogo ${gameNum} (ID: ${realMatchId})...`);

        const endState = JSON.parse(entrada.getData().toString('utf8'));

        let lado_vencedor = 'unknown';
        if (endState.teams) {
            const winTeam = endState.teams.find(t => t.win === true || t.win === "Win");
            if (winTeam) lado_vencedor = (winTeam.teamId || winTeam.teamID) === 100 ? 'blue' : 'red';
        }

        let detailsJson = null;
        if (zipDetails && zipDetails.getEntries) {
            const detEntry = zipDetails.getEntries().find(e => e.entryName.match(new RegExp(`_${gameNum}\\.json$`)));
            if (detEntry) detailsJson = JSON.parse(detEntry.getData().toString('utf8'));
        }

        let eventLines = [];
        if (zipEvents && zipEvents.getEntries) {
            const evtEntry = zipEvents.getEntries().find(e => e.entryName.match(new RegExp(`_${gameNum}\\.jsonl?$`)));
            if (evtEntry) eventLines = evtEntry.getData().toString('utf8').trim().split('\n').filter(l => l).map(l => JSON.parse(l));
        }

        const teamMap = {}; const nameMap = {};
        const getAcronym = (tId) => {
            const p = endState.participants.find(part => (part.teamId === tId || part.teamID === tId));
            const nome = p ? (p.riotIdGameName || p.summonerName || (p.riotId ? p.riotId.displayName : "")) : "";
            return (nome && nome.includes(' ')) ? nome.split(' ')[0] : (tId === 100 ? "BLUE" : "RED");
        };
        teamMap[100] = getAcronym(100); teamMap[200] = getAcronym(200);

        endState.participants.forEach(p => { 
            const pId = p.participantId || p.participantID;
            nameMap[pId] = p.riotIdGameName || p.summonerName || (p.riotId ? p.riotId.displayName : "Unknown"); 
        });

        const patch = endState.gameVersion ? endState.gameVersion.split('.').slice(0,2).join('.') : "N/A";
        let timestamp = endState.gameStartTimestamp || endState.gameCreation || Date.now();
        if (isNaN(new Date(timestamp).getTime())) timestamp = Date.now();
        const finalIsoDate = new Date(timestamp).toISOString();

        // 1. Extrai Dados Brutos
        const statsBrutos = extrairEstatisticasDB(endState, detailsJson, eventLines, realMatchId, teamMap, patch);
        const wardsDB = extrairWardsDB(eventLines, realMatchId, nameMap);
        const objetivosDB = extrairObjetivosDB(eventLines, realMatchId, teamMap, nameMap);
        const draftsDB = extrairDraftsDB(gridState, endState, realMatchId, teamMap, parseInt(gameNum));

        // 2. Extrai Times e Jogadores Únicos
        const times = new Map();
        const jogadores = new Map();
        statsBrutos.forEach(s => {
            if (s.team_acronym) times.set(s.team_acronym, { acronym: s.team_acronym, name: s.team_acronym });
            if (s.puuid) jogadores.set(s.puuid, { puuid: s.puuid, nickname: s.summoner_name, team_acronym: s.team_acronym, primary_role: s.lane });
        });
        
        await supabase.from('teams').upsert(Array.from(times.values()), { onConflict: 'acronym', ignoreDuplicates: true });
        await supabase.from('players').upsert(Array.from(jogadores.values()), { onConflict: 'puuid', ignoreDuplicates: true });

        // 3. Upsert Series e Match
        await supabase.from('series').upsert({ id: seriesId, description: `${teamMap[100]} x ${teamMap[200]}` }, { onConflict: 'id' });
        await supabase.from('matches').upsert({
            id: realMatchId, series_id: seriesId, patch: patch, game_start_time: finalIsoDate, 
            game_type: game_type, split: split, blue_team_tag: teamMap[100], red_team_tag: teamMap[200], winner_side: lado_vencedor
        }, { onConflict: 'id' });

        // 4. Calcula Ratings e Insere Dados Finais
        const statsFinais = await calculateRatings(statsBrutos);
        if (statsFinais.length > 0) await supabase.from('player_stats_detailed').upsert(statsFinais);
        if (wardsDB.length > 0) await supabase.from('match_wards').insert(wardsDB);
        if (objetivosDB.length > 0) await supabase.from('match_objectives').insert(objetivosDB);
        if (draftsDB.length > 0) await supabase.from('match_drafts').insert(draftsDB);

        console.log(`✅ Jogo ${realMatchId} injetado no Supabase com Sucesso!`);
    }
}

// --- RADAR DE VARREDURA DO ANO (COM PAGINAÇÃO) ---
async function buscarEProcessarAnoAteHoje() {
  console.log(`\n🤖 LIGANDO TRATOR DE EXTRAÇÃO (DO INÍCIO DO ANO ATÉ HOJE)...`);
  
  const inicioDoAno = "2026-01-01T00:00:00.000Z";
  const agora = new Date().toISOString(); 
  
  let hasNextPage = true;
  let cursor = null;
  let pagina = 1;
  let totalProcessado = 0;

  while (hasNextPage) {
    const query = `
      query {
        allSeries(
          first: 50, 
          ${cursor ? `after: "${cursor}",` : ""}
          filter: { titleId: 3, startTimeScheduled: { gte: "${inicioDoAno}", lte: "${agora}" } }, 
          orderBy: StartTimeScheduled, orderDirection: DESC
        ) { 
          pageInfo { hasNextPage, endCursor }
          edges { node { id, tournament { name } } } 
        }
      }`;

    try {
      const res = await gridApiGraphQL.post('', { query });
      const data = res.data.data.allSeries;
      
      const partidas = data.edges.map(e => ({ id: e.node.id, campeonato: e.node.tournament?.name || 'Geral' }));
      totalProcessado += partidas.length;
      
      console.log(`\n📄 Lendo Página ${pagina}... (${partidas.length} partidas encontradas)`);
      
      for (const p of partidas) {
          const { data: existe } = await supabase.from('series').select('id').eq('id', p.id).maybeSingle();
          if (!existe) await processarPartidaRecente(p);
          else console.log(`⏩ Série ${p.id} já processada. Pulando...`);
      }
      
      hasNextPage = data.pageInfo.hasNextPage;
      cursor = data.pageInfo.endCursor;
      pagina++;

    } catch (err) { 
      console.error(`❌ Erro no Radar (Página ${pagina}):`, err.message); 
      break;
    }
  }
  
  console.log(`\n🏁 EXECUÇÃO CONCLUÍDA. Total de séries lidas na varredura: ${totalProcessado}.`);
}

buscarEProcessarAnoAteHoje();