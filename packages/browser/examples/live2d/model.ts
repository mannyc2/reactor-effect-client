/**
 * The character page's Live2D adapter: one Cubism model loaded and drawn with
 * the Cubism SDK for Web's own Framework and renderer. Neither the SDK nor any
 * model is in this repository, and this file is built only where someone has
 * linked their own licensed copy of the SDK at `cubism/` (see the README).
 * It is plain DOM and Framework code; the page owns its lifetime and frames.
 */
import { CubismModelSettingJson } from "../cubism/Framework/src/cubismmodelsettingjson";
import { CubismDefaultParameterId } from "../cubism/Framework/src/cubismdefaultparameterid";
import { BreathParameterData, CubismBreath } from "../cubism/Framework/src/effect/cubismbreath";
import { CubismEyeBlink } from "../cubism/Framework/src/effect/cubismeyeblink";
import type { CubismIdHandle } from "../cubism/Framework/src/id/cubismid";
import {
  CubismFramework,
  LogLevel,
  Option as FrameworkOption,
} from "../cubism/Framework/src/live2dcubismframework";
import { CubismMatrix44 } from "../cubism/Framework/src/math/cubismmatrix44";
import type { ACubismMotion } from "../cubism/Framework/src/motion/acubismmotion";
import { CubismUserModel } from "../cubism/Framework/src/model/cubismusermodel";
import type { Adapter, Character } from "../src/Avatar.ts";

/** The motion group a model's idle motions are filed under, by Cubism's convention. */
const idleGroup = "Idle";

const bytes = async (url: URL): Promise<ArrayBuffer> => {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url.pathname} answered ${String(response.status)}`);
  return response.arrayBuffer();
};

/** One premultiplied texture, as the Framework's renderer draws them. */
const texture = async (gl: WebGL2RenderingContext, url: URL): Promise<WebGLTexture> => {
  const image = await createImageBitmap(await (await fetch(url)).blob(), {
    premultiplyAlpha: "premultiply",
  });
  const created = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, created);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, image);
  gl.generateMipmap(gl.TEXTURE_2D);
  gl.bindTexture(gl.TEXTURE_2D, null);
  image.close();
  return created;
};

/** A model with its idle motions, blinking, breathing, physics, pose and lip sync. */
class Model extends CubismUserModel {
  private readonly idle: Array<ACubismMotion> = [];
  private nextIdle = 0;
  private readonly lipSync: Array<CubismIdHandle> = [];
  private readonly textures: Array<WebGLTexture> = [];

  async setUp(gl: WebGL2RenderingContext, setting: CubismModelSettingJson, base: URL) {
    const file = (name: string) => new URL(name, base);
    this.loadModel(await bytes(file(setting.getModelFileName())));
    if (this.getModel() == null) throw new Error("The model's .moc3 did not load");
    if (setting.getPhysicsFileName() !== "") {
      const physics = await bytes(file(setting.getPhysicsFileName()));
      this.loadPhysics(physics, physics.byteLength);
    }
    if (setting.getPoseFileName() !== "") {
      const pose = await bytes(file(setting.getPoseFileName()));
      this.loadPose(pose, pose.byteLength);
    }
    if (setting.getEyeBlinkParameterCount() > 0) this._eyeBlink = CubismEyeBlink.create(setting);
    const eyeBlink = Array.from({ length: setting.getEyeBlinkParameterCount() }, (_, index) =>
      setting.getEyeBlinkParameterId(index),
    );
    const ids = CubismFramework.getIdManager();
    this._breath = CubismBreath.create();
    this._breath.setParameters([
      new BreathParameterData(ids.getId(CubismDefaultParameterId.ParamAngleX), 0, 12, 6.5, 0.5),
      new BreathParameterData(ids.getId(CubismDefaultParameterId.ParamAngleY), 0, 6, 3.5, 0.5),
      new BreathParameterData(ids.getId(CubismDefaultParameterId.ParamAngleZ), 0, 8, 5.5, 0.5),
      new BreathParameterData(ids.getId(CubismDefaultParameterId.ParamBodyAngleX), 0, 3, 15.5, 0.5),
      new BreathParameterData(ids.getId(CubismDefaultParameterId.ParamBreath), 0.5, 0.5, 3.2, 1),
    ]);
    for (let index = 0; index < setting.getLipSyncParameterCount(); index++)
      this.lipSync.push(setting.getLipSyncParameterId(index));
    // A model that names no lip-sync parameter still has Cubism's standard mouth.
    if (this.lipSync.length === 0)
      this.lipSync.push(ids.getId(CubismDefaultParameterId.ParamMouthOpenY));
    for (let index = 0; index < setting.getMotionCount(idleGroup); index++) {
      const motion = await bytes(file(setting.getMotionFileName(idleGroup, index)));
      const loaded = this.loadMotion(
        motion,
        motion.byteLength,
        `${idleGroup}_${String(index)}`,
        undefined,
        undefined,
        setting,
        idleGroup,
        index,
      );
      if (loaded != null) {
        loaded.setEffectIds(eyeBlink, this.lipSync);
        this.idle.push(loaded);
      }
    }

    this.getModel().saveParameters();

    this.createRenderer(gl.drawingBufferWidth, gl.drawingBufferHeight);
    const renderer = this.getRenderer();
    renderer.startUp(gl);
    renderer.setIsPremultipliedAlpha(true);
    for (let index = 0; index < setting.getTextureCount(); index++) {
      const created = await texture(gl, file(setting.getTextureFileName(index)));
      this.textures.push(created);
      renderer.bindTexture(index, created);
    }
  }

  update(seconds: number, mouth: number) {
    const model = this.getModel();
    model.loadParameters();
    if (this._motionManager.isFinished() && this.idle.length > 0) {
      const motion = this.idle[this.nextIdle % this.idle.length];
      this.nextIdle++;
      this._motionManager.startMotionPriority(motion, false, 1);
    }
    const moved = this._motionManager.updateMotion(model, seconds);
    model.saveParameters();
    // A motion that animates the eyes owns them while it plays.
    if (!moved) this._eyeBlink?.updateParameters(model, seconds);
    this._breath.updateParameters(model, seconds);
    this._physics?.evaluate(model, seconds);
    // Playback owns the mouth, including silence, even when an idle motion animates it.
    for (const id of this.lipSync) model.setParameterValueById(id, mouth * 0.8);
    this._pose?.updateParameters(model, seconds);
    model.update();
  }

  draw(gl: WebGL2RenderingContext, shaders: string) {
    const canvas = gl.canvas;
    if (!(canvas instanceof HTMLCanvasElement)) return;
    const scale = window.devicePixelRatio;
    const width = Math.max(1, Math.round(canvas.clientWidth * scale));
    const height = Math.max(1, Math.round(canvas.clientHeight * scale));
    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
      this.setRenderTargetSize(width, height);
    }
    gl.viewport(0, 0, width, height);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    // The model fills the canvas's height, or its width when that is the narrower side.
    const projection = new CubismMatrix44();
    if (width > height) projection.scale(height / width, 1);
    else projection.scale(1, width / height);
    projection.multiplyByMatrix(this.getModelMatrix());
    const renderer = this.getRenderer();
    renderer.setMvpMatrix(projection);
    renderer.setRenderState(null, [0, 0, width, height]);
    renderer.drawModel(shaders);
  }

  releaseAll(gl: WebGL2RenderingContext) {
    this.release();
    for (const created of this.textures) gl.deleteTexture(created);
    this.textures.length = 0;
  }
}

export const load: Adapter["load"] = async (options): Promise<Character> => {
  if (!CubismFramework.isStarted()) {
    const settings = new FrameworkOption();
    settings.logFunction = (message: string) => console.warn(message);
    settings.loggingLevel = LogLevel.LogLevel_Warning;
    CubismFramework.startUp(settings);
  }
  if (!CubismFramework.isInitialized()) CubismFramework.initialize();
  const gl = options.canvas.getContext("webgl2", { alpha: true, premultipliedAlpha: true });
  if (gl === null) throw new Error("This browser has no WebGL 2");
  const base = new URL(options.model, window.location.href);
  const json = await bytes(base);
  const setting = new CubismModelSettingJson(json, json.byteLength);
  const model = new Model();
  try {
    await model.setUp(gl, setting, base);
  } catch (error) {
    model.releaseAll(gl);
    throw error;
  }
  return {
    update: (seconds, mouth) => model.update(seconds, mouth),
    draw: () => model.draw(gl, options.shaders),
    release: () => model.releaseAll(gl),
  };
};
