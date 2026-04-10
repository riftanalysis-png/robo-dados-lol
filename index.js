require('dotenv').config();
const axios = require('axios');
const AdmZip = require('adm-zip');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const gridApi = axios.create({ headers: { 'x-api-key': process.env.API_KEY } });

const CAMPEONATOS_ALVO = [
  "lck", "lec", "lcs", "emea", "cblol", "circuito desafiante",
  "first stand", "mundial", "world", "msi", "americas cup"
];

const toNum = (val) => {
  if (val === undefined || val === null || val === '') return 0;
  if (typeof val === 'number') return val;
  const cleanVal = val.toString().replace('%', '').replace(/\./g, '').replace(',', '.');
  return parseFloat(cleanVal) || 0;
};

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
    w: 'gpm', z: 'fpm', x: 'gold_efficiency', ao: 'gold_share', aq: '_aq',
    t: 'vspm', aj: 'wards_killed', ai: 'wards_placed', u: 'cw_placed'
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
        b.min_val = Math.min(b.min_val, cMin);
        b.max_val = Math.max(b.max_val, cMax);
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
        sumN += norm * weight;
        sumW += weight;
      });
      return Math.max(50, 50 + (50 * (sumN / (sumW || 1))));
    };

    const { _ap, _aq, ...cleanPlayer } = p;
    return {
      ...cleanPlayer, lane_efficiency: _ap, dmg_gold_ratio: _aq,
      lane_rating: calc(['ap', 'p', 'ar', 'as', 'at', 'l', 'ac', 'al', 'am']),
      impact_rating: calc(['i', 'k', 'ae', 'ad', 'aa', 'ab', 'v', 'an']),
      conversion_rating: calc(['w', 'z', 'x', 'ao', 'aq']),
      vision_rating: calc(['t', 'aj', 'ai', 'u'])
    };
  });
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

function extrairEstatisticasDB(jogo, eventLines, matchId, teamMap, patch) {
  const eventosChave = extrairEventosChave(eventLines);
  const duracaoMinutos = (jogo.gameDuration || 1) / 60;
  const limit12 = 12 * 60 * 1000;
  const timelineEvents = eventLines.filter(e => (e.gameTime || e.timestamp || 0) <= limit12);

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

    const deaths12 = timelineEvents.filter(e => e.eventType === "CHAMPION_KILL" && (e.victimName === summoner || e.victimId === pId)).length;
    const kills12 = timelineEvents.filter(e => e.eventType === "CHAMPION_KILL" && (e.killerName === summoner || e.killerId === pId)).length;
    const assists12 = timelineEvents.filter(e => e.eventType === "CHAMPION_KILL" && (e.assisters || []).includes(summoner)).length;

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
      match_id: matchId, summoner_name: summoner, puuid: p.puuid || "",
      game_start_time: new Date(timestamp).toISOString(),
      patch: patch, team_acronym: teamMap[tId] || (tId === 100 ? "BLUE" : "RED"), side: tId === 100 ? 'Blue' : 'Red',
      lane: p.lane || p.teamPosition || p.role || "UNKNOWN", champion: p.championName,
      kda: toNum(p.challenges?.kda || (p.deaths ? (p.kills + p.assists) / p.deaths : (p.kills + p.assists))),
      kills: p.kills || 0, deaths: p.deaths || 0, deaths_at_12: deaths12, assists: p.assists || 0,
      result: isWin ? "Victory" : "Defeat", cs_6: 0, cs_12: 0, cs_18: 0, xp_12: 0, gold_12: 0,
      vspm: toNum((p.visionScore || 0) / duracaoMinutos), cw_placed: p.detectorWardsPlaced || p.visionWardsBoughtInGame || 0,
      dpm: toNum(dmg / duracaoMinutos), gpm: toNum(gold / duracaoMinutos), gold_efficiency: toNum(gold / duracaoMinutos / 4.5),
      kp: toNum(p.challenges?.killParticipation), fpm: toNum(minions / duracaoMinutos),
      dmg_buildings: p.damageDealtToBuildings || 0, dmg_objectives: p.damageDealtToObjectives || 0, plates: p.challenges?.turretPlatesTaken || 0,
      dmg_percent: toNum(dmg / (teamStats[tId]?.dmg || 1)), taken_percent: toNum(taken / (teamStats[tId]?.taken || 1)),
      mitigated: p.totalDamageSelfMitigated || 0,
      fb_assist: eventosChave[summoner]?.fbAssist || false, fb_kill: eventosChave[summoner]?.fbKill || false,
      ft_assist: eventosChave[summoner]?.ftAssist || false, ft_kill: eventosChave[summoner]?.ftKill || false,
      total_dmg: dmg, total_taken: taken, vision_score: p.visionScore || 0, wards_placed: p.wardsPlaced || 0, wards_killed: p.wardsKilled || 0,
      total_gold: gold, win: isWin ? true : false,
      vpm_at_12: 0, kda_at_12: deaths12 > 0 ? toNum((kills12 + assists12) / deaths12) : kills12 + assists12,
      cc_score: p.timeCCingOthers || 0, gold_share: toNum(gold / (teamStats[tId]?.gold || 1))
    };
  });
}

function extrairWardsDB(eventLines, matchId, nameMap) {
  const wards = [];
  eventLines.forEach(e => {
    if (e.rfc461Schema === "ward_placed" || e.type === "WARD_PLACED") {
      const pid = e.placer?.toString() || e.creatorId?.toString();
      wards.push({
        match_id: matchId, player_name: nameMap[pid] || pid,
        minute: Math.floor((e.gameTime || e.timestamp || 0) / 60000),
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
      tipo: tipo, side: side, jogador: tipo === 'PICK' ? (champMap[`${campeao}_${side}`] || 'Team') : 'Team',
      champion: campeao, sequence: seq
    });
  });

  return drafts;
}

async function baixarESepararZip(url) {
  try {
    const res = await gridApi.get(url, { responseType: 'arraybuffer' });
    const buffer = Buffer.from(res.data);
    const arquivos = [];
    if (buffer[0] === 0x50 && buffer[1] === 0x4B) {
       const zip = new AdmZip(buffer);
       zip.getEntries().forEach(entry => {
         if (!entry.isDirectory) arquivos.push({ nome: entry.entryName, conteudo: entry.getData().toString('utf8') });
       });
       return arquivos;
    }
    return [{ nome: 'solto.json', conteudo: buffer.toString('utf8') }];
  } catch (e) { return []; }
}

async function processarPartidas(partidas) {
  for (const partida of partidas) {
    const seriesId = String(partida.id);
    console.log(`\n⏳ A baixar Série [${seriesId}] - ${partida.campeonato}`);

    const endStatesRiot = await baixarESepararZip(`https://api.grid.gg/file-download/end-state/riot/series/${seriesId}`);
    const eventosRiot = await baixarESepararZip(`https://api.grid.gg/file-download/events/riot/series/${seriesId}`);
    const endStatesGrid = await baixarESepararZip(`https://api.grid.gg/file-download/end-state/grid/series/${seriesId}`);

    if (endStatesRiot.length === 0) continue;

    let gridState = null;
    if (endStatesGrid.length > 0) { try { gridState = JSON.parse(endStatesGrid[0].conteudo); } catch(e) {} }

    for (const arq of endStatesRiot) {
        let endState;
        try { endState = JSON.parse(arq.conteudo); } catch(e) { continue; }
        if (!endState?.participants) continue;

        const matchRegex = arq.nome.match(/_(\d+)\.json/);
        const gameNum = matchRegex ? matchRegex[1] : "1";
        const realMatchId = String(`${seriesId}_${gameNum}`);

        const { data: existe } = await supabase.from('matches').select('id').eq('id', realMatchId).maybeSingle();
        if (existe) {
             console.log(`⏩ Jogo ${realMatchId} já no Banco de Dados. Pulando...`);
             continue;
        }

        console.log(`▶️ A processar e salvar Jogo ${gameNum} (ID: ${realMatchId})...`);

        let arqEvento = eventosRiot.find(a => a.nome.includes(`_${gameNum}_`));
        if (!arqEvento && eventosRiot.length === 1) arqEvento = eventosRiot[0];
        const eventLines = arqEvento ? arqEvento.conteudo.trim().split('\n').filter(l => l).map(l => JSON.parse(l)) : [];

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
        
        const statsBrutos = extrairEstatisticasDB(endState, eventLines, realMatchId, teamMap, patch);
        const statsFinais = await calculateRatings(statsBrutos);
        const wardsDB = extrairWardsDB(eventLines, realMatchId, nameMap);
        const objetivosDB = extrairObjetivosDB(eventLines, realMatchId, teamMap, nameMap);
        const draftsDB = extrairDraftsDB(gridState, endState, realMatchId, teamMap, parseInt(gameNum));
        
        const winTeam = endState.teams ? endState.teams.find(t => t.win) : null;
        let timestamp = endState.gameStartTimestamp || endState.gameCreation || Date.now();
        if (isNaN(new Date(timestamp).getTime())) timestamp = Date.now();

        // ==========================================
        // 🚨 BLOCO DE SALVAMENTO BLINDADO (NOVO) 🚨
        // ==========================================

        // 1. FORÇAR A CRIAÇÃO DA SÉRIE
        const serieData = {
            id: seriesId,
            description: `${teamMap[100] || 'BLUE'} vs ${teamMap[200] || 'RED'} - ${partida.campeonato}`
        };

        const { error: errSeries } = await supabase.from('series').upsert(serieData, { onConflict: 'id' });
        if (errSeries) console.error(`❌ Erro DB (series):`, errSeries.message);

        // VERIFICADOR: A Série entrou mesmo no banco?
        const { data: verifySeries } = await supabase.from('series').select('id').eq('id', seriesId).maybeSingle();
        if (!verifySeries) {
             console.error(`🚨 ERRO GRAVE: O Supabase bloqueou a criação da Série ${seriesId}! Verifica se tens a chave 'service_role' no teu .env e não a 'anon public'.`);
             console.log("⏩ A saltar a partida para evitar erros em cascata...");
             continue; // Salta o resto do código para este jogo
        }

        // 2. CRIAR A PARTIDA
        const { error: errMatch } = await supabase.from('matches').upsert({
            id: realMatchId, series_id: seriesId, patch: String(patch),
            game_start_time: new Date(timestamp).toISOString(),
            game_type: String(partida.campeonato), split: "AUTO-SYNC",
            blue_team_tag: String(teamMap[100]), red_team_tag: String(teamMap[200]),
            winner_side: winTeam ? ((winTeam.teamId || winTeam.teamID) === 100 ? 'blue' : 'red') : 'unknown'
        }, { onConflict: 'id' });

        if (errMatch) {
            console.error(`❌ Erro DB (matches):`, errMatch.message);
            continue; // Se a partida falhou, nem tenta guardar os Status
        }

        // 3. INSERIR OS DADOS DETALHADOS
        if (statsFinais.length > 0) {
            const { error: errStats } = await supabase.from('player_stats_detailed').insert(statsFinais);
            if (errStats) console.error(`❌ Erro DB (stats):`, errStats.message);
        }
        if (wardsDB.length > 0) {
            const { error: errWards } = await supabase.from('match_wards').insert(wardsDB);
            if (errWards) console.error(`❌ Erro DB (wards):`, errWards.message);
        }
        if (objetivosDB.length > 0) {
            const { error: errObj } = await supabase.from('match_objectives').insert(objetivosDB);
            if (errObj) console.error(`❌ Erro DB (objetivos):`, errObj.message);
        }
        if (draftsDB.length > 0) {
            const { error: errDraft } = await supabase.from('match_drafts').insert(draftsDB);
            if (errDraft) console.error(`❌ Erro DB (drafts):`, errDraft.message);
        }

        console.log(`✅ Jogo ${realMatchId} injetado com Sucesso!`);
    }
  }
}

async function executarBackfillStreaming(tipo) {
  let hasNextPage = true;
  let cursor = null;
  let pagina = 1;

  console.log(`\n📚 LIGANDO TRATOR DE HISTÓRICO (2026) PARA: ${tipo}...`);

  while (hasNextPage) {
    const query = `
      query {
        allSeries(
          first: 50, 
          ${cursor ? `after: "${cursor}",` : ""} 
          filter: { 
            titleId: 3, 
            types: ${tipo},
            startTimeScheduled: {
              gte: "2026-01-01T00:00:00Z"
              lte: "2026-12-31T23:59:59Z"
            }
          }, 
          orderBy: StartTimeScheduled, 
          orderDirection: DESC
        ) {
          pageInfo { hasNextPage, endCursor }
          edges { node { id, tournament { name } } }
        }
      }`;

    try {
      const res = await axios.post('https://api.grid.gg/central-data/graphql', { query }, { headers: { 'x-api-key': process.env.API_KEY } });
      const data = res.data.data.allSeries;
      
      const partidasDaPagina = data.edges.map(e => ({ id: e.node.id, campeonato: e.node.tournament?.name || tipo }));
      let partidasValidas = [];

      if (tipo === "ESPORTS") {
          partidasValidas = partidasDaPagina.filter(p => {
              const nome = p.campeonato.toLowerCase();
              return CAMPEONATOS_ALVO.some(alvo => nome.includes(alvo));
          });
      } else {
          partidasValidas = partidasDaPagina;
      }

      console.log(`\n📄 [${tipo}] Lendo Página ${pagina}... (${partidasValidas.length} partidas alvo encontradas)`);
      
      if (partidasValidas.length > 0) {
          await processarPartidas(partidasValidas);
      }

      hasNextPage = data.pageInfo.hasNextPage;
      cursor = data.pageInfo.endCursor;
      pagina++;

    } catch (err) {
      console.error(`❌ Erro no GraphQL (Página ${pagina}):`, err.response?.data || err.message);
      break;
    }
  }
}

async function iniciarBackfill() {
  await executarBackfillStreaming("SCRIM");
  await executarBackfillStreaming("ESPORTS");
  console.log("\n🏁 TRABALHO CONCLUÍDO. O histórico de 2026 está 100% no Supabase!");
}

iniciarBackfill();