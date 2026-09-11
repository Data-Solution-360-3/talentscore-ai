/* assessment_monitor.js — the MCQ assessment's activity monitor.
   The interview page's proven detector bundle (tab/focus with durations,
   copy/paste flags, reload/reopen, inactivity), DOM-only — no camera, no
   WebRTC. Data shapes match the interview's proctoring exactly:
   flags[{t,type,reason,dur?}], counts{type:n}, durations{key:s} — so the
   server validator and the recruiter renderer stay one family.
   HONESTY: everything here is a REVIEW SIGNAL for a human. Nothing blocks,
   ejects, or scores. The page discloses all of it before question 1. */
(function(){
  'use strict';
  var M = {
    startMs: 0, flags: [], counts: {}, durations: {},
    awaySince: 0, awayReason: '', lastActivity: 0,
    pasteMode: 'restrict', currentQ: -1, inactTimer: null, armed: false,
  };
  function nowT(){ return Math.round((Date.now() - M.startMs) / 1000); }
  function flag(type, reason, dur){
    M.counts[type] = (M.counts[type] || 0) + 1;
    var f = { t: nowT(), type: type, reason: reason };
    if (dur != null) f.dur = Math.round(dur);
    M.flags.push(f);
    if (M.flags.length > 120) M.flags.shift();   // bounded; server caps again
  }
  function qLabel(){ return M.currentQ >= 0 ? ' (question ' + (M.currentQ + 1) + ')' : ''; }

  function onAway(reason){
    if (!M.armed || M.awaySince) return;
    M.awaySince = Date.now(); M.awayReason = reason;
  }
  function onBack(){
    if (!M.awaySince) return;
    var dur = (Date.now() - M.awaySince) / 1000; M.awaySince = 0;
    var k = M.awayReason === 'window lost focus' ? 'focus_loss' : 'tab_away';
    M.durations[k] = (M.durations[k] || 0) + dur;
    flag('tab_switch', 'left the assessment (' + M.awayReason + ')' + qLabel(), dur);
  }
  function bump(){ M.lastActivity = Date.now(); }
  function inactTick(){
    if (!M.armed || M.awaySince) return;          // away time is counted separately
    var idle = (Date.now() - M.lastActivity) / 1000;
    if (idle >= 90){
      flag('inactivity', 'no input for ' + Math.round(idle) + 's' + qLabel(), idle);
      M.lastActivity = Date.now();                // one flag per stretch
    }
  }

  window.AssessMonitor = {
    /* opts: { pasteMode: 'restrict'|'monitor', storageKey: string } */
    start: function(opts){
      opts = opts || {};
      M.pasteMode = opts.pasteMode === 'monitor' ? 'monitor' : 'restrict';
      M.startMs = Date.now(); M.lastActivity = Date.now(); M.armed = true;
      // Reload/reopen: a sessionStorage counter survives refreshes — each
      // restart of this monitor past the first is a recorded reload.
      try {
        var k = (opts.storageKey || 'am') + '-loads';
        var n = parseInt(sessionStorage.getItem(k) || '0') + 1;
        sessionStorage.setItem(k, String(n));
        if (n > 1) flag('refresh', 'page reloaded/reopened during the assessment (load #' + n + ')');
      } catch(_){}
      document.addEventListener('visibilitychange', function(){
        if (document.hidden) onAway('tab hidden'); else onBack();
      });
      window.addEventListener('blur', function(){ onAway('window lost focus'); });
      window.addEventListener('focus', function(){ onBack(); });
      ['mousemove','keydown','pointerdown','scroll','touchstart'].forEach(function(ev){
        window.addEventListener(ev, bump, {passive: true});
      });
      document.addEventListener('copy', function(){ flag('copy', 'copied from the assessment' + qLabel()); });
      document.addEventListener('cut',  function(){ flag('cut',  'cut from the assessment' + qLabel()); });
      document.addEventListener('paste', function(e){
        flag('paste', (M.pasteMode === 'restrict' ? 'paste attempted (blocked)'
                                                  : 'paste (allowed, recorded)') + qLabel());
        if (M.pasteMode === 'restrict') e.preventDefault();
      });
      M.inactTimer = setInterval(inactTick, 10000);
    },
    noteQuestion: function(i){ M.currentQ = i; bump(); },
    /* FULL proctoring level (per-job, recruiter-chosen, DISCLOSED on the
       instruction page): camera + screen requested, periodic still snapshots
       (<=12, alternating cam/scr) uploaded to `uploadUrl`. The interview's
       exact approach. DENIAL FLAGS, NEVER BLOCKS — the test always continues;
       a human reviews the flags. Light level never calls this. */
    startMedia: async function(uploadUrl){
      M.media = { cam:null, scr:null, camV:null, scrV:null, up:0, tick:0, timer:null };
      function vid(stream){
        var v = document.createElement('video');
        v.muted = true; v.playsInline = true; v.style.display = 'none';
        v.srcObject = stream; document.body.appendChild(v); v.play().catch(function(){});
        return v;
      }
      try {
        M.media.cam = await navigator.mediaDevices.getUserMedia(
          {video: {width: {ideal: 640}, height: {ideal: 480}}});
        M.media.camV = vid(M.media.cam);
        var ct = M.media.cam.getVideoTracks()[0];
        if (ct){
          ct.addEventListener('ended', function(){ flag('camera_off', 'camera turned off during the assessment'); });
          ct.addEventListener('mute',  function(){ flag('camera_off', 'camera muted during the assessment'); });
        }
      } catch(_){ flag('camera_off', 'camera denied or unavailable at start'); }
      try {
        M.media.scr = await navigator.mediaDevices.getDisplayMedia(
          {video: {displaySurface: 'monitor', frameRate: {ideal: 2}}});
        M.media.scrV = vid(M.media.scr);
        var st = M.media.scr.getVideoTracks()[0];
        var surf = (st && st.getSettings && st.getSettings().displaySurface) || '';
        if (surf && surf !== 'monitor') flag('partial_share', 'shared a ' + surf + ', not the full monitor');
        if (st) st.addEventListener('ended', function(){ flag('share_stopped', 'screen share stopped'); });
      } catch(_){ flag('share_stopped', 'screen share declined'); }
      function grab(v){
        if (!v || !v.videoWidth) return null;
        var c = document.createElement('canvas');
        c.width = 320; c.height = Math.round(320 * v.videoHeight / v.videoWidth);
        c.getContext('2d').drawImage(v, 0, 0, c.width, c.height);
        return c.toDataURL('image/jpeg', 0.6);
      }
      async function up(kind, v){
        if (M.media.up >= 12) return;
        var d = grab(v); if (!d) return;
        try {
          var r = await fetch(uploadUrl, {method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({img: d, kind: kind})});
          if (r.ok){
            var j = await r.json().catch(function(){ return {}; });
            if (j.stored) M.media.up++;
            else if (j.cap_reached) M.media.up = 12;
          }
        } catch(_){}
      }
      async function snapTick(){
        M.media.tick++;
        await up('cam', M.media.camV);
        if (M.media.tick % 2 === 0) await up('scr', M.media.scrV);
      }
      M.media.timer = setInterval(snapTick, 90000);
      setTimeout(snapTick, 8000);
    },
    summary: function(){
      if (M.media){
        try { clearInterval(M.media.timer); } catch(_){}
        ['cam','scr'].forEach(function(k){
          try { (M.media[k] && M.media[k].getTracks() || []).forEach(function(t){ t.stop(); }); } catch(_){}
        });
      }
      return this._summarize();
    },
    _summarize: function(){
      onBack();   // close any open away-stretch into the totals
      return {
        enabled: true, flags_schema: 1, mode: M.pasteMode,
        flags: M.flags, counts: M.counts,
        durations: (function(d){ var o = {}; for (var k in d) o[k] = Math.round(d[k]); return o; })(M.durations),
        total_seconds: nowT(),
      };
    },
  };
})();
