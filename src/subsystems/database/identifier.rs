use serde::Serialize;
use std::borrow::Cow;
use utoipa::ToSchema;

pub trait IdentifierPrefix {
    fn prefix() -> char;
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UsernameIdentifier;
impl IdentifierPrefix for UsernameIdentifier {
    #[inline(always)]
    fn prefix() -> char {
        'u'
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseIdentifier;
impl IdentifierPrefix for DatabaseIdentifier {
    #[inline(always)]
    fn prefix() -> char {
        'd'
    }
}

pub type UserIdentifier = StructuredIdentifier<UsernameIdentifier>;
pub type DbIdentifier = StructuredIdentifier<DatabaseIdentifier>;

// <p><4b>_<1..=23b>
// u11111111_hello
#[derive(ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[schema(as = String)]
pub struct StructuredIdentifier<P: IdentifierPrefix> {
    user_short_uuid: u32,
    user_len: u8,
    user: [u8; 23],
    #[schema(ignore)]
    _marker: std::marker::PhantomData<P>,
}

impl<P: IdentifierPrefix> std::fmt::Display for StructuredIdentifier<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{:08x}_{}",
            P::prefix(),
            self.user_short_uuid,
            self.user()
        )
    }
}

impl<P: IdentifierPrefix> std::str::FromStr for StructuredIdentifier<P> {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let prefix = P::prefix();
        let Some(trimmed) = s.strip_prefix(prefix) else {
            return Err(format!("identifier must start with '{prefix}'").into());
        };
        let Some((short_uuid, user)) = trimmed.split_once('_') else {
            return Err("invalid identifier format".into());
        };

        if short_uuid.len() != 8 || !short_uuid.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid short UUID".into());
        }
        if user.len() < 2 {
            return Err("user part too short".into());
        }

        let user_bytes = user.as_bytes();
        let mut user_array = [0; 23];
        let Some(user_array_slice) = user_array.get_mut(..user_bytes.len()) else {
            return Err("user part too long".into());
        };
        user_array_slice.copy_from_slice(user_bytes);

        Ok(Self {
            user_short_uuid: u32::from_str_radix(short_uuid, 16)
                .map_err(|_| "invalid short UUID")?,
            user_len: user_bytes.len() as u8,
            user: user_array,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<P: IdentifierPrefix> Serialize for StructuredIdentifier<P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<P: IdentifierPrefix> StructuredIdentifier<P> {
    pub fn from_parts(short_uuid: u32, user: &str) -> Result<Self, anyhow::Error> {
        if user.len() < 2 {
            anyhow::bail!("user part too short");
        }

        let mut user_array = [0; 23];
        let Some(user_array_slice) = user_array.get_mut(..user.len()) else {
            anyhow::bail!("user part too long");
        };
        user_array_slice.copy_from_slice(user.as_bytes());

        Ok(Self {
            user_short_uuid: short_uuid,
            user_len: user.len() as u8,
            user: user_array,
            _marker: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn short_uuid(&self) -> u32 {
        self.user_short_uuid
    }

    #[inline]
    pub fn user(&self) -> &str {
        // SAFETY: The username is always valid UTF-8, and the length is correct.
        unsafe { std::str::from_utf8_unchecked(self.user.get_unchecked(..self.user_len as usize)) }
    }
}
