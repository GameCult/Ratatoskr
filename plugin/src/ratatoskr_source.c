/* The OBS source. Owns no transport, no reassembly, no discovery: all of that
 * is ratatoskr-core behind ratatoskr.h. What this file owns is the OBS
 * lifecycle — properties, activation, and handing what the core delivers to
 * OBS in the shape OBS wants.
 *
 * Video goes through a private child `ffmpeg_source` reading a raw H.264
 * byte stream from a loopback UDP port the core relays into; OBS's own
 * decoder does the decoding and this source draws the child. Audio is PCM
 * and goes straight to obs_source_output_audio. That is the arrangement the
 * previous plugin proved in the field; what it hand-rolled underneath it is
 * what the core replaces.
 */

#include <obs-module.h>
#include <util/platform.h>
#include <util/threading.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ratatoskr.h"

OBS_DECLARE_MODULE()
OBS_MODULE_USE_DEFAULT_LOCALE("ratatoskr", "en-US")

#define SETTING_ODIN "odin"
#define SETTING_RECEIVER_ID "receiver_id"
#define SETTING_STREAM "stream_id"
#define SETTING_VIDEO_SOURCE "video_source_id"
#define SETTING_AUDIO_SOURCE "audio_source_id"
#define SETTING_VIDEO_CODEC "video_codec"
#define SETTING_AUDIO_CODEC "audio_codec"
#define SETTING_BITRATE "video_bitrate_kbps"
#define SETTING_LATENCY "latency_budget_ms"
#define SETTING_STATUS "status"

/* Odin's stable rendezvous route as Idunn publishes it (nginx stream proxy on
 * yggdrasil); the daemon's own port behind it moves per release. */
#define DEFAULT_ODIN "rudp://10.77.0.1:17971"
#define NONE_ID ""

struct ratatoskr_source {
	obs_source_t *source;

	/* settings as last applied */
	char *odin;
	char *receiver_id;
	char *stream_id;
	char *video_source_id;
	char *audio_source_id;
	char *video_codec;
	char *audio_codec;
	unsigned int bitrate_kbps;
	unsigned int latency_ms;

	/* what Odin last said */
	RatatoskrCatalog *catalog;
	pthread_mutex_t catalog_mutex;

	/* the live subscription */
	RatatoskrHandle *receiver;
	obs_source_t *video_child;
	uint16_t relay_port;
	uint32_t audio_sample_rate;
	uint32_t audio_channels;
	size_t catalog_index;

	pthread_t pump;
	volatile bool pump_running;
	bool active;

	uint8_t *buffer;
	size_t buffer_capacity;
};

static void log_last_error(const char *what)
{
	char message[512];
	ratatoskr_last_error(message, sizeof(message));
	blog(LOG_WARNING, "[ratatoskr] %s: %s", what, message);
}

static void set_string(char **slot, const char *value)
{
	bfree(*slot);
	*slot = bstrdup(value ? value : "");
}

/* ---- catalog ---- */

static void refresh_catalog(struct ratatoskr_source *ctx)
{
	char state_dir[1024];
	const char *config = obs_module_config_path("");
	snprintf(state_dir, sizeof(state_dir), "%s", config ? config : ".");
	bfree((void *)config);
	os_mkdirs(state_dir);

	RatatoskrCatalog *fresh = NULL;
	int rc = ratatoskr_catalog_pull(ctx->odin, ctx->receiver_id, state_dir, &fresh);
	if (rc != RATATOSKR_OK) {
		log_last_error("catalog pull from Odin failed");
		return;
	}
	pthread_mutex_lock(&ctx->catalog_mutex);
	RatatoskrCatalog *old = ctx->catalog;
	ctx->catalog = fresh;
	pthread_mutex_unlock(&ctx->catalog_mutex);
	ratatoskr_catalog_close(old);
	blog(LOG_INFO, "[ratatoskr] Odin advertises %zu stream(s), %zu malformed", ratatoskr_catalog_count(fresh),
	     ratatoskr_catalog_rejected(fresh));
}

static bool find_stream(struct ratatoskr_source *ctx, const char *stream_id, size_t *out_index,
			RatatoskrStreamInfo *out_info)
{
	if (!ctx->catalog || !stream_id || !*stream_id)
		return false;
	size_t count = ratatoskr_catalog_count(ctx->catalog);
	for (size_t i = 0; i < count; i++) {
		RatatoskrStreamInfo info;
		if (ratatoskr_catalog_stream(ctx->catalog, i, &info) != RATATOSKR_OK)
			continue;
		if (strcmp(info.stream_id, stream_id) == 0) {
			*out_index = i;
			*out_info = info;
			return true;
		}
	}
	return false;
}

/* ---- media ---- */

static void release_video_child(struct ratatoskr_source *ctx)
{
	if (!ctx->video_child)
		return;
	obs_source_remove_active_child(ctx->source, ctx->video_child);
	obs_source_release(ctx->video_child);
	ctx->video_child = NULL;
}

static void create_video_child(struct ratatoskr_source *ctx)
{
	char url[64];
	snprintf(url, sizeof(url), "udp://127.0.0.1:%u", (unsigned)ctx->relay_port);
	obs_data_t *settings = obs_data_create();
	obs_data_set_bool(settings, "is_local_file", false);
	obs_data_set_string(settings, "input", url);
	obs_data_set_string(settings, "input_format", "h264");
	obs_data_set_string(settings, "ffmpeg_options",
			    "fflags=nobuffer flags=low_delay probesize=32768 analyzeduration=0");
	obs_data_set_bool(settings, "restart_on_activate", true);
	obs_data_set_bool(settings, "clear_on_media_end", false);
	obs_data_set_bool(settings, "close_when_inactive", false);
	obs_data_set_bool(settings, "hw_decode", true);
	ctx->video_child = obs_source_create_private("ffmpeg_source", "Ratatoskr video", settings);
	obs_data_release(settings);
	if (!ctx->video_child) {
		blog(LOG_WARNING, "[ratatoskr] could not create the ffmpeg_source child for %s", url);
		return;
	}
	obs_source_add_active_child(ctx->source, ctx->video_child);
}

static void output_audio(struct ratatoskr_source *ctx, const uint8_t *pcm, size_t len, int64_t pts_ns)
{
	if (ctx->audio_channels == 0 || ctx->audio_sample_rate == 0)
		return;
	size_t frame_bytes = sizeof(float) * ctx->audio_channels;
	if (len < frame_bytes)
		return;
	struct obs_source_audio audio = {0};
	audio.data[0] = pcm;
	audio.frames = (uint32_t)(len / frame_bytes);
	audio.samples_per_sec = ctx->audio_sample_rate;
	audio.format = AUDIO_FORMAT_FLOAT;
	audio.speakers = ctx->audio_channels == 1 ? SPEAKERS_MONO : SPEAKERS_STEREO;
	audio.timestamp = pts_ns > 0 ? (uint64_t)pts_ns : os_gettime_ns();
	obs_source_output_audio(ctx->source, &audio);
}

static void *pump_loop(void *data)
{
	struct ratatoskr_source *ctx = data;
	os_set_thread_name("ratatoskr-pump");
	while (ctx->pump_running) {
		int waiting = ratatoskr_receiver_poll(ctx->receiver);
		if (waiting < 0) {
			log_last_error("poll failed");
			os_sleep_ms(50);
			continue;
		}
		for (;;) {
			size_t len = 0;
			int kind = -1;
			int64_t pts_ns = 0;
			int rc = ratatoskr_receiver_next_payload(ctx->receiver, ctx->buffer, ctx->buffer_capacity, &len,
								&kind, &pts_ns);
			if (rc == RATATOSKR_ERR_BUFFER_TOO_SMALL) {
				ctx->buffer_capacity = len + len / 2;
				ctx->buffer = brealloc(ctx->buffer, ctx->buffer_capacity);
				continue;
			}
			if (rc != RATATOSKR_OK)
				break;
			/* video already went to the relay; the child decodes it */
			if (kind == RATATOSKR_KIND_AUDIO)
				output_audio(ctx, ctx->buffer, len, pts_ns);
		}
		os_sleep_ms(2);
	}
	return NULL;
}

static void stop_stream(struct ratatoskr_source *ctx)
{
	if (ctx->pump_running) {
		ctx->pump_running = false;
		pthread_join(ctx->pump, NULL);
	}
	if (ctx->catalog && ctx->receiver) {
		pthread_mutex_lock(&ctx->catalog_mutex);
		if (ratatoskr_request_stop(ctx->catalog, ctx->catalog_index) != RATATOSKR_OK)
			log_last_error("stop request failed");
		pthread_mutex_unlock(&ctx->catalog_mutex);
	}
	release_video_child(ctx);
	if (ctx->receiver) {
		ratatoskr_receiver_close(ctx->receiver);
		ctx->receiver = NULL;
	}
}

static void start_stream(struct ratatoskr_source *ctx)
{
	stop_stream(ctx);
	if (!ctx->catalog)
		refresh_catalog(ctx);

	size_t index;
	RatatoskrStreamInfo info;
	pthread_mutex_lock(&ctx->catalog_mutex);
	bool found = find_stream(ctx, ctx->stream_id, &index, &info);
	pthread_mutex_unlock(&ctx->catalog_mutex);
	if (!found) {
		blog(LOG_WARNING, "[ratatoskr] stream %s is not advertised through %s", ctx->stream_id, ctx->odin);
		return;
	}
	bool wants_video = ctx->video_source_id[0] != 0;
	bool wants_audio = ctx->audio_source_id[0] != 0;
	if (!wants_video && !wants_audio) {
		blog(LOG_INFO, "[ratatoskr] neither video nor audio selected for %s", ctx->stream_id);
		return;
	}

	ctx->catalog_index = index;
	ctx->audio_sample_rate = info.audio_sample_rate;
	ctx->audio_channels = info.audio_channels;
	ctx->relay_port = wants_video ? ratatoskr_free_udp_port() : 0;

	/* Ask first, then dial: the request spins the producer up and the dial
	 * is repeated until it answers. Nothing here listens. */
	pthread_mutex_lock(&ctx->catalog_mutex);
	int rc = ratatoskr_request_start(ctx->catalog, index, wants_video ? ctx->video_source_id : NONE_ID,
					 wants_audio ? ctx->audio_source_id : NONE_ID, ctx->video_codec, ctx->audio_codec,
					 ctx->bitrate_kbps, ctx->latency_ms);
	pthread_mutex_unlock(&ctx->catalog_mutex);
	if (rc != RATATOSKR_OK) {
		log_last_error("start request failed");
		return;
	}
	blog(LOG_INFO, "[ratatoskr] asked %s for %s (video=%s audio=%s %s/%s %u kbps, %u ms); dialling %s",
	     info.producer_id, ctx->stream_id, wants_video ? ctx->video_source_id : "-",
	     wants_audio ? ctx->audio_source_id : "-", ctx->video_codec, ctx->audio_codec, ctx->bitrate_kbps,
	     ctx->latency_ms, info.media_endpoint);

	char relay[64] = {0};
	if (wants_video)
		snprintf(relay, sizeof(relay), "127.0.0.1:%u", (unsigned)ctx->relay_port);
	rc = ratatoskr_receiver_open(info.media_endpoint, ctx->receiver_id, info.media_connection_id,
				     wants_video ? relay : NULL, &ctx->receiver);
	if (rc != RATATOSKR_OK) {
		log_last_error("receiver open failed");
		stop_stream(ctx);
		return;
	}
	if (wants_video)
		create_video_child(ctx);

	ctx->pump_running = true;
	if (pthread_create(&ctx->pump, NULL, pump_loop, ctx) != 0) {
		ctx->pump_running = false;
		blog(LOG_ERROR, "[ratatoskr] could not start the pump thread");
		stop_stream(ctx);
	}
}

/* ---- obs_source_info ---- */

static const char *ratatoskr_get_name(void *unused)
{
	UNUSED_PARAMETER(unused);
	return obs_module_text("RatatoskrSource");
}

static void ratatoskr_get_defaults(obs_data_t *settings)
{
	obs_data_set_default_string(settings, SETTING_ODIN, DEFAULT_ODIN);
	obs_data_set_default_string(settings, SETTING_RECEIVER_ID, "obs");
	obs_data_set_default_string(settings, SETTING_VIDEO_CODEC, "h264");
	obs_data_set_default_string(settings, SETTING_AUDIO_CODEC, "pcm-f32le-interleaved");
	obs_data_set_default_int(settings, SETTING_BITRATE, 0);
	obs_data_set_default_int(settings, SETTING_LATENCY, 0);
}

static void apply_settings(struct ratatoskr_source *ctx, obs_data_t *settings)
{
	set_string(&ctx->odin, obs_data_get_string(settings, SETTING_ODIN));
	set_string(&ctx->receiver_id, obs_data_get_string(settings, SETTING_RECEIVER_ID));
	set_string(&ctx->stream_id, obs_data_get_string(settings, SETTING_STREAM));
	set_string(&ctx->video_source_id, obs_data_get_string(settings, SETTING_VIDEO_SOURCE));
	set_string(&ctx->audio_source_id, obs_data_get_string(settings, SETTING_AUDIO_SOURCE));
	set_string(&ctx->video_codec, obs_data_get_string(settings, SETTING_VIDEO_CODEC));
	set_string(&ctx->audio_codec, obs_data_get_string(settings, SETTING_AUDIO_CODEC));
	ctx->bitrate_kbps = (unsigned int)obs_data_get_int(settings, SETTING_BITRATE);
	ctx->latency_ms = (unsigned int)obs_data_get_int(settings, SETTING_LATENCY);
}

static void *ratatoskr_create(obs_data_t *settings, obs_source_t *source)
{
	struct ratatoskr_source *ctx = bzalloc(sizeof(*ctx));
	ctx->source = source;
	pthread_mutex_init(&ctx->catalog_mutex, NULL);
	ctx->buffer_capacity = 256 * 1024;
	ctx->buffer = bmalloc(ctx->buffer_capacity);
	apply_settings(ctx, settings);
	return ctx;
}

static void ratatoskr_destroy(void *data)
{
	struct ratatoskr_source *ctx = data;
	stop_stream(ctx);
	ratatoskr_catalog_close(ctx->catalog);
	pthread_mutex_destroy(&ctx->catalog_mutex);
	bfree(ctx->buffer);
	bfree(ctx->odin);
	bfree(ctx->receiver_id);
	bfree(ctx->stream_id);
	bfree(ctx->video_source_id);
	bfree(ctx->audio_source_id);
	bfree(ctx->video_codec);
	bfree(ctx->audio_codec);
	bfree(ctx);
}

static void ratatoskr_update(void *data, obs_data_t *settings)
{
	struct ratatoskr_source *ctx = data;
	apply_settings(ctx, settings);
	if (ctx->active)
		start_stream(ctx);
}

static void ratatoskr_activate(void *data)
{
	struct ratatoskr_source *ctx = data;
	ctx->active = true;
	start_stream(ctx);
}

static void ratatoskr_deactivate(void *data)
{
	struct ratatoskr_source *ctx = data;
	ctx->active = false;
	stop_stream(ctx);
}

static uint32_t ratatoskr_get_width(void *data)
{
	struct ratatoskr_source *ctx = data;
	return ctx->video_child ? obs_source_get_width(ctx->video_child) : 0;
}

static uint32_t ratatoskr_get_height(void *data)
{
	struct ratatoskr_source *ctx = data;
	return ctx->video_child ? obs_source_get_height(ctx->video_child) : 0;
}

static void ratatoskr_video_render(void *data, gs_effect_t *effect)
{
	UNUSED_PARAMETER(effect);
	struct ratatoskr_source *ctx = data;
	if (ctx->video_child)
		obs_source_video_render(ctx->video_child);
}

/* ---- properties ---- */

static void fill_list(obs_property_t *list, size_t count, const char *const *ids, const char *const *labels)
{
	obs_property_list_clear(list);
	obs_property_list_add_string(list, obs_module_text("Ratatoskr.None"), NONE_ID);
	for (size_t i = 0; i < count; i++)
		obs_property_list_add_string(list, labels ? labels[i] : ids[i], ids[i]);
}

static void fill_from_catalog(struct ratatoskr_source *ctx, obs_properties_t *props, const char *stream_id)
{
	obs_property_t *streams = obs_properties_get(props, SETTING_STREAM);
	obs_property_t *video = obs_properties_get(props, SETTING_VIDEO_SOURCE);
	obs_property_t *audio = obs_properties_get(props, SETTING_AUDIO_SOURCE);
	obs_property_t *vcodec = obs_properties_get(props, SETTING_VIDEO_CODEC);
	obs_property_t *acodec = obs_properties_get(props, SETTING_AUDIO_CODEC);

	pthread_mutex_lock(&ctx->catalog_mutex);
	obs_property_list_clear(streams);
	size_t count = ctx->catalog ? ratatoskr_catalog_count(ctx->catalog) : 0;
	RatatoskrStreamInfo selected;
	bool have_selected = false;
	for (size_t i = 0; i < count; i++) {
		RatatoskrStreamInfo info;
		if (ratatoskr_catalog_stream(ctx->catalog, i, &info) != RATATOSKR_OK)
			continue;
		char label[512];
		snprintf(label, sizeof(label), "%s — %s (%s)", info.producer_id, info.label, info.state);
		obs_property_list_add_string(streams, label, info.stream_id);
		if (stream_id && strcmp(info.stream_id, stream_id) == 0) {
			selected = info;
			have_selected = true;
		}
	}
	if (have_selected) {
		fill_list(video, selected.video_source_count, selected.video_source_ids, selected.video_source_labels);
		fill_list(audio, selected.audio_source_count, selected.audio_source_ids, selected.audio_source_labels);
		obs_property_list_clear(vcodec);
		for (size_t i = 0; i < selected.video_codec_count; i++)
			obs_property_list_add_string(vcodec, selected.video_codecs[i], selected.video_codecs[i]);
		obs_property_list_clear(acodec);
		for (size_t i = 0; i < selected.audio_codec_count; i++)
			obs_property_list_add_string(acodec, selected.audio_codecs[i], selected.audio_codecs[i]);
	} else {
		fill_list(video, 0, NULL, NULL);
		fill_list(audio, 0, NULL, NULL);
		obs_property_list_clear(vcodec);
		obs_property_list_clear(acodec);
	}
	pthread_mutex_unlock(&ctx->catalog_mutex);
}

static bool on_stream_changed(void *data, obs_properties_t *props, obs_property_t *property, obs_data_t *settings)
{
	UNUSED_PARAMETER(property);
	struct ratatoskr_source *ctx = data;
	fill_from_catalog(ctx, props, obs_data_get_string(settings, SETTING_STREAM));
	return true;
}

static bool on_refresh(obs_properties_t *props, obs_property_t *property, void *data)
{
	UNUSED_PARAMETER(property);
	struct ratatoskr_source *ctx = data;
	obs_data_t *settings = obs_source_get_settings(ctx->source);
	set_string(&ctx->odin, obs_data_get_string(settings, SETTING_ODIN));
	set_string(&ctx->receiver_id, obs_data_get_string(settings, SETTING_RECEIVER_ID));
	refresh_catalog(ctx);
	fill_from_catalog(ctx, props, obs_data_get_string(settings, SETTING_STREAM));
	obs_data_release(settings);
	return true;
}

static obs_properties_t *ratatoskr_get_properties(void *data)
{
	struct ratatoskr_source *ctx = data;
	obs_properties_t *props = obs_properties_create();

	obs_properties_add_text(props, SETTING_ODIN, obs_module_text("Ratatoskr.Odin"), OBS_TEXT_DEFAULT);
	obs_properties_add_text(props, SETTING_RECEIVER_ID, obs_module_text("Ratatoskr.ReceiverId"), OBS_TEXT_DEFAULT);
	obs_properties_add_button(props, "refresh", obs_module_text("Ratatoskr.Refresh"), on_refresh);

	obs_property_t *streams = obs_properties_add_list(props, SETTING_STREAM, obs_module_text("Ratatoskr.Stream"),
							  OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);
	obs_properties_add_list(props, SETTING_VIDEO_SOURCE, obs_module_text("Ratatoskr.VideoSource"),
				OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);
	obs_properties_add_list(props, SETTING_AUDIO_SOURCE, obs_module_text("Ratatoskr.AudioSource"),
				OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);
	obs_properties_add_list(props, SETTING_VIDEO_CODEC, obs_module_text("Ratatoskr.VideoCodec"),
				OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);
	obs_properties_add_list(props, SETTING_AUDIO_CODEC, obs_module_text("Ratatoskr.AudioCodec"),
				OBS_COMBO_TYPE_LIST, OBS_COMBO_FORMAT_STRING);
	obs_properties_add_int(props, SETTING_BITRATE, obs_module_text("Ratatoskr.VideoBitrateKbps"), 0, 100000, 500);
	obs_properties_add_int(props, SETTING_LATENCY, obs_module_text("Ratatoskr.LatencyBudgetMs"), 0, 2000, 25);

	if (ctx) {
		if (!ctx->catalog)
			refresh_catalog(ctx);
		obs_property_set_modified_callback2(streams, on_stream_changed, ctx);
		fill_from_catalog(ctx, props, ctx->stream_id);
	}
	return props;
}

static struct obs_source_info ratatoskr_source_info = {
	.id = "ratatoskr_media_stream",
	.type = OBS_SOURCE_TYPE_INPUT,
	.output_flags = OBS_SOURCE_VIDEO | OBS_SOURCE_AUDIO | OBS_SOURCE_CUSTOM_DRAW | OBS_SOURCE_DO_NOT_DUPLICATE,
	.get_name = ratatoskr_get_name,
	.create = ratatoskr_create,
	.destroy = ratatoskr_destroy,
	.update = ratatoskr_update,
	.get_defaults = ratatoskr_get_defaults,
	.get_properties = ratatoskr_get_properties,
	.activate = ratatoskr_activate,
	.deactivate = ratatoskr_deactivate,
	.get_width = ratatoskr_get_width,
	.get_height = ratatoskr_get_height,
	.video_render = ratatoskr_video_render,
	.icon_type = OBS_ICON_TYPE_MEDIA,
};

bool obs_module_load(void)
{
	obs_register_source(&ratatoskr_source_info);
	blog(LOG_INFO, "[ratatoskr] loaded");
	return true;
}
