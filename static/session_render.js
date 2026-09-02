// Shared renderers for a live-interview session document.
// Used by BOTH /viva-live/sessions (the full page) and the candidate report
// card's Interview / Proctoring tabs — one implementation, two surfaces.
// Everything renders from the session doc as returned by
// GET /api/viva-live/sessions/{id} (owner-gated).
window.SessionRender = (function(){
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

  // ── Segment scores + per-dimension evidence (spoken / typed / scenario) ──
  function scoresHTML(s){
    const res = s.score_result, wr = s.written_result, sr = s.scenario_result;
    let html = '';
    if(res){
      if(wr || sr){
        html += `<div style="display:flex;gap:1.6rem;align-items:baseline;flex-wrap:wrap;margin-bottom:.2rem">
          ${s.interview_score!=null?`<div><div style="font-size:10.5px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--green,#16A34A)">Interview — combined</div>
            <span class="score-big">${s.interview_score}<span style="font-size:.9rem;color:var(--t3)">/100</span></span>
            <div style="font-size:.72rem;color:var(--t3)">spoken ×${(s.interview_parts&&s.interview_parts.weights&&s.interview_parts.weights.spoken)||0.6} + written ×${(s.interview_parts&&s.interview_parts.weights&&s.interview_parts.weights.written)||0.4} — fixed weights, never question counts</div></div>`:''}
          <div><div style="font-size:10.5px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3)">Spoken segment</div>
            <span class="score-big">${res.overall}<span style="font-size:.9rem;color:var(--t3)">/100</span></span></div>
          ${wr?`<div><div style="font-size:10.5px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3)">Typed segment</div>
            <span class="score-big">${wr.segment_score}<span style="font-size:.9rem;color:var(--t3)">/100</span></span></div>`:''}
          ${sr?`<div><div style="font-size:10.5px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3)">Written scenario</div>
            <span class="score-big">${sr.overall}<span style="font-size:.9rem;color:var(--t3)">/100</span></span></div>`:''}
          <span style="font-size:.78rem;color:var(--t3);max-width:280px;line-height:1.5">Different rubrics, shown separately — not averaged into one number.</span>
        </div>
        <div style="font-size:.85rem;color:var(--t2);margin-bottom:.2rem">${esc(res.summary||'')}</div>`;
      }else{
        html += `<div style="display:flex;align-items:baseline;gap:12px;margin-bottom:.2rem">
          <span class="score-big">${res.overall}<span style="font-size:.9rem;color:var(--t3)">/100</span></span>
          <span style="font-size:.85rem;color:var(--t2)">${esc(res.summary||'')}</span></div>`;
      }
      html += `<div class="evrow"><span>scorer ${esc(res.scorer_version)}</span><span>blend ${res.blend?res.blend.strict+'/'+res.blend.generous:''}</span>${wr?`<span>written scorer ${esc(wr.scorer_version||'')}</span>`:''}${sr?`<span>scenario scorer ${esc(sr.scorer_version||'')}</span>`:''}</div>`;
      if(wr||sr) html += `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.9rem 0 .1rem">Spoken segment — conversation rubric</div>`;
      html += `<div style="margin:.6rem 0 1rem">` + res.dimensions.map(d => `
        <div class="dim">
          <div class="dim-top"><span>${esc(d.name)}</span><span>${d.score}/20 <span style="font-weight:500;color:var(--t3);font-size:11px">(strict ${d.strict} · generous ${d.generous})</span></span></div>
          <div class="dim-bar"><i style="width:${(d.score/20*100).toFixed(0)}%"></i></div>
          <div class="dim-reason">${esc(d.reason)}</div>
          ${d.evidence?`<div class="dim-ev">“${esc(d.evidence)}”</div>`:''}
        </div>`).join('') + `</div>`;
      if(wr){
        html += `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.4rem 0 .1rem">Typed segment — written rubric</div>`;
        html += (wr.answers||[]).map(a => `
          <div style="background:var(--s2);border:1px solid var(--border);border-radius:var(--r-lg,12px);padding:.8rem .95rem;margin:.6rem 0">
            <div style="display:flex;justify-content:space-between;gap:10px;font-size:.88rem;font-weight:700;color:var(--navy)">
              <span>${esc(a.question||'Typed question')}</span><span>${a.overall}/100</span></div>
            ${(a.dimensions||[]).map(d => `
            <div class="dim">
              <div class="dim-top" style="font-size:.85rem"><span>${esc(d.name)}</span><span>${d.score}/20</span></div>
              <div class="dim-bar"><i style="width:${(d.score/20*100).toFixed(0)}%"></i></div>
              <div class="dim-reason">${esc(d.reason||'')}</div>
            </div>`).join('')}
            ${a.summary?`<div style="font-size:.82rem;color:var(--t2);margin-top:.35rem">${esc(a.summary)}</div>`:''}
          </div>`).join('');
      }else if(s.written_error){
        html += `<div class="notice">Typed segment scoring failed: ${esc(s.written_error)}</div>`;
      }
      if(sr){
        html += `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.9rem 0 .1rem">Written scenario — scored as a set</div>
        <div style="background:rgba(22,163,74,.06);border:1px solid rgba(22,163,74,.22);border-radius:var(--r-lg,12px);padding:.8rem .95rem;margin:.5rem 0">
          <div style="font-size:10.5px;font-weight:800;letter-spacing:.5px;text-transform:uppercase;color:var(--t3);margin-bottom:.3rem">The scenario shown to the candidate</div>
          <div style="font-size:.9rem;line-height:1.6;color:var(--text);white-space:pre-wrap">${esc(sr.scenario||'')}</div>
        </div>`;
        html += (sr.qa||[]).map((p,i) => `
          <div style="background:var(--s2);border:1px solid var(--border);border-radius:var(--r-lg,12px);padding:.8rem .95rem;margin:.5rem 0">
            <div style="font-size:.88rem;font-weight:700;color:var(--navy)">Q${i+1}. ${esc(p.question||'')}</div>
            <div style="font-size:.87rem;color:var(--t2);line-height:1.6;margin-top:.35rem;white-space:pre-wrap">${esc(p.answer||'')}</div>
            ${(sr.per_answer_notes&&sr.per_answer_notes[i])?`<div style="font-size:.8rem;color:var(--t3);margin-top:.35rem">${esc(sr.per_answer_notes[i])}</div>`:''}
          </div>`).join('');
        html += `<div style="margin:.6rem 0 1rem">` + (sr.dimensions||[]).map(d => `
          <div class="dim">
            <div class="dim-top"><span>${esc(d.name)}</span><span>${d.score}/20 <span style="font-weight:500;color:var(--t3);font-size:11px">(strict ${d.strict} · generous ${d.generous})</span></span></div>
            <div class="dim-bar"><i style="width:${(d.score/20*100).toFixed(0)}%"></i></div>
            <div class="dim-reason">${esc(d.reason||'')}</div>
          </div>`).join('')
          + `${sr.summary?`<div style="font-size:.83rem;color:var(--t2);margin-top:.3rem">${esc(sr.summary)}</div>`:''}</div>`;
      }else if(s.scenario_error){
        html += `<div class="notice">Scenario segment scoring failed: ${esc(s.scenario_error)}</div>`;
      }
    }else{
      html += `<div class="notice">Scoring: ${esc(s.score_status||'pending')}${s.score_error?' — '+esc(s.score_error):''}. Reload in a moment.</div>`;
    }
    return html;
  }

  function statsHTML(s){
    return `<div class="evrow" style="margin-bottom:.6rem">
      <span>${s.questions_asked||0} of ${(s.config&&s.config.max_turns)||'?'} questions</span>
      <span>${s.recoveries||0} network drop(s) survived</span>
      <span>${s.barge_ins||0} barge-in(s)</span>
      <span>patience: ${esc((s.config&&s.config.vad)||'—')}</span>
      <span>${esc(s.status||'')}</span></div>` + costHTML(s);
  }

  // ── Owner-only cost telemetry. Both surfaces that call this (recruiter
  //    results + candidate report) are recruiter-facing; candidates never see
  //    it. USD is an estimate from the server's named rate constants. ──
  function costHTML(s){
    const u = s.usage;
    if(!u || typeof u!=='object') return '';
    const usd = (typeof u.est_usd==='number') ? u.est_usd : 0;
    const hit = (typeof u.cache_hit_pct==='number') ? u.cache_hit_pct : 0;
    const k = n => (Math.abs(+n||0)>=1000 ? ((+n||0)/1000).toFixed(1)+'k' : String(+n||0));
    const tip = `input ${k(u.input)} tok (cached ${k(u.cached_input)}) · output ${k(u.output)} tok`
              + ` · audio in/out ${k(u.in_audio)}/${k(u.out_audio)} · ${u.responses||0} responses`
              + ` · rates ${u.rates_version||'—'}`;
    return `<div class="evrow" style="margin:-.3rem 0 .6rem;font-size:11px;color:var(--t3)" title="${esc(tip)}">
      <span>💵 Est. cost <b>$${usd.toFixed(2)}</b></span>
      <span>cache hit ${hit}%</span>
      <span style="opacity:.65">estimated · owner only</span></div>`;
  }

  // ── Color-coded transcript: navy interviewer bubbles, green candidate ──
  function transcriptHTML(s){
    let html = `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.8rem 0 .3rem">Transcript</div>`;
    html += (s.transcript||[]).map(t => `
      <div class="turn ${t.role==='ai'?'ai':'you'}">
        <span class="who">${t.role==='ai'?('🤖 '+esc((s.config&&s.config.interviewer_name)||'AI Interviewer')):'🗣 Candidate'}${t.mode==='typed'?(t.scen?' · ⌨ scenario answer':' · ⌨ typed answer'):''}${t.mode==='scenario'?' · 📋 scenario shown':''}${t.pasted?' · <b style="color:var(--orange2,#E16A1F)">📋 contains pasted content</b>':''}</span>${esc(t.text)}
      </div>`).join('') || '<div class="notice">No transcript captured.</div>';
    return html;
  }

  // ── Proctoring: coverage timeline + frames strip, honest in every state.
  //    snapsId must be unique per surface (page vs modal). ──
  function proctoringHTML(s, snapsId){
    const pr = s.proctoring;
    let html = '';
    if(pr && pr.enabled){
      const mmss = t => Math.floor(t/60)+':'+String(t%60).padStart(2,'0');
      // ── Anti-cheat review flags: counts + coverage numbers, honest labels. ──
      const c = pr.counts||{}, du = pr.durations||{};
      const flagChips = [
        ['camera_off','Camera off', du.camera_off],
        ['tab_switch','Tab switch / focus lost', du.tab_away],
        ['paste','Pasted into answer'], ['copy','Copied'], ['cut','Cut'],
        ['no_face','No face detected'], ['multi_face','Multiple faces'],
        ['look_away','Look-away', du.look_away],
        ['partial_share','Partial screen share'], ['share_stopped','Screen share stopped'],
      ].filter(([k])=> (c[k]||0) > 0).map(([k,label,dur])=>
        `<span style="display:inline-block;background:var(--og);color:var(--orange2);border:1px solid var(--ob);border-radius:9999px;padding:2px 10px;font-size:11.5px;font-weight:700;margin:0 4px 4px 0">${esc(label)} ×${c[k]}${dur?` · ${dur}s`:''}</span>`
      ).join('');
      const camCov = (pr.cam_on_seconds!=null && pr.total_seconds) ? `camera on <b>${mmss(pr.cam_on_seconds)}</b> of ${mmss(pr.total_seconds)}` : '';
      const scrCov = (pr.scr_on_seconds!=null && pr.total_seconds && pr.final_scr && pr.final_scr!=='unavailable') ? `screen shared <b>${mmss(pr.scr_on_seconds)}</b> of ${mmss(pr.total_seconds)}` : '';
      const cov = [camCov, scrCov].filter(Boolean).join(' · ');
      // Three states, honestly distinguished (T1): flags recorded and none
      // raised != flags never stored. Sessions saved before the candidate
      // path persisted flags (flags_schema absent) must NEVER read as clean.
      const flagsStored = pr.flags_schema != null || pr.flags != null;
      const flagsEmptyMsg = flagsStored
        ? '<span style="color:var(--t3)">No review flags raised.</span>'
        : '<span style="color:var(--t3)"><b>Flag capture wasn’t stored for sessions recorded before 3 Sep 2026</b> — the absence of flags here is a storage gap, not a clean signal. Read the transcript and frames.</span>';
      const flagsBlock = `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.9rem 0 .3rem">Review flags <span style="text-transform:none;letter-spacing:0;font-weight:500">— monitored + flagged, never blocked; expect false positives, read the transcript &amp; frames</span></div>
        <div style="background:var(--s2);border:1px solid var(--border);border-radius:var(--r-lg,12px);padding:.7rem .9rem;font-size:.83rem;color:var(--t2);line-height:1.7">
          ${cov?`<div style="margin-bottom:.45rem">${cov}</div>`:''}
          ${flagChips || flagsEmptyMsg}
          <div style="font-size:11px;color:var(--t3);margin-top:.5rem;line-height:1.5">Browser proctoring cannot see a second device, a second monitor, or the candidate&rsquo;s other tabs — these flags raise the cost of casual cheating and catch obvious cases; they are not proof.</div>
        </div>`;
      html += flagsBlock;
      html += `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.8rem 0 .3rem">Proctoring coverage</div>
      <div style="background:var(--s2);border:1px solid var(--border);border-radius:var(--r-lg,12px);padding:.7rem .9rem;font-size:.83rem;color:var(--t2);line-height:1.7">`;
      html += (pr.coverage_segments||[]).map(cs =>
        `<div><b style="font-variant-numeric:tabular-nums">${mmss(cs.t)}</b> — ${esc(cs.change)}<span style="color:var(--t3)"> · ${esc(cs.reason)}</span></div>`).join('')
        || '<div>No coverage events recorded.</div>';
      html += `<div style="margin-top:.4rem;color:var(--t3)">ended: camera ${esc(pr.final_cam||'—')}, screen ${esc(pr.final_scr||'—')} · ${pr.snapshots||0} watchdog frame grab(s) (not stored — the stored review frames are counted below) · ${(((pr.cam_bytes||0)+(pr.scr_bytes||0))/1048576).toFixed(1)} MB streamed for the watchdog</div>
      <div style="font-size:11px;color:var(--t3);margin-top:.35rem;line-height:1.5">Continuous camera/screen streams run in-browser to drive the watchdog and are <b>not persisted</b> — full video storage needs the object-storage phase. The frames stored for review load below.</div>
      <div id="${snapsId}" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:.5rem;font-size:11.5px;color:var(--t3)">Loading stored frames…</div>
      </div>`;
    }else{
      html += `<div style="font-size:11px;font-weight:800;letter-spacing:.6px;text-transform:uppercase;color:var(--t3);margin:.8rem 0 .3rem">Proctoring</div>
      <div style="background:var(--s2);border:1px dashed var(--border);border-radius:var(--r-lg,12px);padding:.8rem .95rem;font-size:.85rem;color:var(--t2);line-height:1.6">
        🎥 <b>No proctoring for this interview.</b> It was set to <b>Off</b> in the interview setup when this candidate's link was created — no camera or screen was requested, so there is nothing to review here.
        <div style="font-size:11.5px;color:var(--t3);margin-top:.35rem">To proctor future interviews on a job: job card → Live interview → Proctoring → Snapshot or Continuous → Save.</div>
      </div>`;
    }
    return html;
  }

  async function loadSnapshots(sessionId, snapsId){
    const wrap = document.getElementById(snapsId);
    if(!wrap) return;
    try{
      const r = await fetch('/api/viva-live/sessions/'+sessionId+'/snapshots', {credentials:'include'});
      const d = r.ok ? await r.json() : {snapshots:[]};
      const snaps = d.snapshots||[];
      wrap.innerHTML = snaps.length
        ? snaps.map(sn => `<figure style="margin:0">
            <img src="${sn.img}" style="height:96px;border-radius:8px;border:1px solid var(--border);display:block" alt="proctoring frame">
            <figcaption style="font-size:9.5px;color:var(--t3);margin-top:2px">${esc((sn.created_at||'').replace('T',' ').slice(5,16))} · ${sn.kind==='scr'?'screen':'camera'}${sn.label?` · <b style="color:var(--orange2)">${esc(String(sn.label).replace('_',' '))}</b>`:''}</figcaption>
          </figure>`).join('')
        : '⚠ Proctoring was ON but no frames are stored — either the camera was denied, this session predates frame storage (before Sep 1), or capture failed. For a fresh session this is a bug worth reporting.';
    }catch(_){ wrap.textContent = 'Could not load stored frames.'; }
  }

  return {scoresHTML, statsHTML, costHTML, transcriptHTML, proctoringHTML, loadSnapshots, esc};
})();
